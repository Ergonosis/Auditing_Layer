#!/usr/bin/env python3
"""
Automated testing framework for Ergonosis Auditing demo pipeline.

This script:
1. Discovers all test datasets in ria_data/
2. Runs the pipeline on each dataset
3. Calculates confusion matrix (TP/FP/TN/FN)
4. Generates benchmark report
"""

import os
import sys
import json
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
RIA_DATA_DIR = project_root / "ria_data"
BENCHMARK_RESULTS_DIR = project_root / "tests" / "benchmark_results"
RUN_DEMO_SCRIPT = project_root / "scripts" / "run_demo.py"


def discover_datasets() -> List[Path]:
    """
    Discover all dataset directories with metadata.json in ria_data/

    Returns:
        List of dataset directory paths
    """
    print("\n" + "="*70)
    print("  Discovering Test Datasets")
    print("="*70)

    datasets = []

    if not RIA_DATA_DIR.exists():
        print(f"❌ Data directory not found: {RIA_DATA_DIR}")
        return datasets

    for item in RIA_DATA_DIR.iterdir():
        if item.is_dir():
            metadata_file = item / "metadata.json"
            if metadata_file.exists():
                datasets.append(item)
                print(f"✅ Found dataset: {item.name}")

    if not datasets:
        print("⚠️  No datasets found with metadata.json")
    else:
        print(f"\n📊 Total datasets discovered: {len(datasets)}")

    return sorted(datasets, key=lambda x: x.name)


def load_metadata(dataset_path: Path) -> Dict:
    """Load metadata.json from dataset directory"""
    metadata_file = dataset_path / "metadata.json"

    with open(metadata_file, 'r') as f:
        return json.load(f)


def run_pipeline_on_dataset(dataset_path: Path) -> Dict:
    """
    Run demo pipeline on specific dataset using subprocess

    Args:
        dataset_path: Path to dataset directory

    Returns:
        Dict with pipeline results including flags
    """
    dataset_name = dataset_path.name
    print(f"\n{'='*70}")
    print(f"  Running Pipeline: {dataset_name}")
    print(f"{'='*70}")

    # Create temp file for JSON output
    output_file = f"/tmp/demo_output_{dataset_name}_{int(time.time())}.json"

    # Set up environment
    env = os.environ.copy()
    env['DEMO_DATA_DIR'] = str(dataset_path)
    env['TEST_MODE'] = 'true'  # Enable flag collection
    env['STATE_BACKEND'] = 'memory'
    env['LOG_LEVEL'] = 'WARNING'  # Reduce noise
    env.setdefault('UNIFICATION_USER_EMAIL', 'test@ergonosis.com')

    print(f"📂 Dataset: {dataset_path}")
    print(f"💾 Output: {output_file}")
    print(f"\n⏳ Running pipeline (timeout: 10 minutes)...")

    start_time = time.time()

    try:
        # Run pipeline as subprocess
        result = subprocess.run(
            [
                sys.executable,
                str(RUN_DEMO_SCRIPT),
                '--data-dir', str(dataset_path),
                '--json-output', output_file
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=600,  # 10 min timeout
            cwd=str(project_root)
        )

        duration = time.time() - start_time

        if result.returncode != 0:
            print(f"❌ Pipeline failed with exit code {result.returncode}")
            print(f"STDERR: {result.stderr}")
            return {
                'status': 'error',
                'error': result.stderr,
                'duration_seconds': duration,
                'flags': []
            }

        # Load JSON output
        if Path(output_file).exists():
            with open(output_file, 'r') as f:
                pipeline_output = json.load(f)

            pipeline_output['duration_seconds'] = duration
            print(f"✅ Pipeline completed in {duration:.1f}s")
            print(f"📋 Flags created: {len(pipeline_output.get('flags', []))}")

            # Clean up temp file
            Path(output_file).unlink()

            return pipeline_output
        else:
            print(f"⚠️  JSON output file not found: {output_file}")
            return {
                'status': 'success',
                'duration_seconds': duration,
                'flags': []
            }

    except subprocess.TimeoutExpired:
        print(f"❌ Pipeline timed out after 5 minutes")
        return {
            'status': 'timeout',
            'duration_seconds': 300,
            'flags': []
        }
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'duration_seconds': time.time() - start_time,
            'flags': []
        }


def calculate_confusion_matrix(flags: List[Dict], ground_truth: Dict) -> Dict:
    """
    Calculate confusion matrix metrics

    TP: Flagged AND corrupted
    FP: Flagged but NOT corrupted
    TN: Not flagged AND not corrupted
    FN: Not flagged but WAS corrupted

    Args:
        flags: List of flags created by pipeline
        ground_truth: Ground truth metadata from dataset

    Returns:
        Dict with TP/FP/TN/FN and metrics
    """
    corrupted_set = set(ground_truth.get('corrupted_row_ids', []))
    flagged_set = set([f['txn_id'] for f in flags])
    total_rows = ground_truth.get('total_rows', 0)

    TP = len(flagged_set & corrupted_set)  # Intersection
    FP = len(flagged_set - corrupted_set)  # Flagged but not corrupted
    FN = len(corrupted_set - flagged_set)  # Corrupted but not flagged
    TN = total_rows - len(flagged_set | corrupted_set)  # Neither

    # Metrics are undefined when there are no corrupted rows (0/0 division)
    # Track this explicitly rather than reporting 0% which is misleading
    has_corrupted = len(corrupted_set) > 0
    precision = TP / (TP + FP) if (TP + FP) > 0 else None
    recall = TP / (TP + FN) if (TP + FN) > 0 else None
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision is not None and recall is not None and (precision + recall) > 0) else None
    accuracy = (TP + TN) / total_rows if total_rows > 0 else 0

    return {
        'TP': TP,
        'FP': FP,
        'TN': TN,
        'FN': FN,
        'precision': precision,
        'recall': recall,
        'f1_score': f1_score,
        'accuracy': accuracy,
        'metrics_defined': has_corrupted  # False for clean datasets with no corrupted rows
    }


def print_dataset_results(dataset_name: str, metadata: Dict, pipeline_results: Dict, confusion_matrix: Dict):
    """Print formatted results for a single dataset"""
    print(f"\n{'='*70}")
    print(f"  Results: {dataset_name}")
    print(f"{'='*70}")

    ground_truth = metadata.get('ground_truth', {})
    flags = pipeline_results.get('flags', [])

    print(f"\n📊 Ground Truth:")
    print(f"  • Total Rows: {ground_truth.get('total_rows', 0):,}")
    print(f"  • Corrupted Rows: {ground_truth.get('corrupted_rows', 0):,} ({ground_truth.get('corrupted_rows', 0) / ground_truth.get('total_rows', 1) * 100:.1f}%)")
    print(f"  • Corruption Type: {metadata.get('corruption_type', 'unknown')}")

    print(f"\n🔍 Pipeline Results:")
    print(f"  • Status: {pipeline_results.get('status', 'unknown').upper()}")
    print(f"  • Duration: {pipeline_results.get('duration_seconds', 0):.1f}s")
    print(f"  • Flags Created: {len(flags):,}")

    if len(flags) > 0:
        # Count severity distribution
        severity_counts = {}
        for flag in flags:
            severity = flag.get('severity', 'UNKNOWN')
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        print(f"  • Severity Breakdown:")
        for severity, count in sorted(severity_counts.items()):
            print(f"    - {severity}: {count}")

    print(f"\n📈 Confusion Matrix:")
    print(f"  • True Positives (TP):  {confusion_matrix['TP']:>5}  (Correctly flagged corrupted rows)")
    print(f"  • False Positives (FP): {confusion_matrix['FP']:>5}  (Incorrectly flagged clean rows)")
    print(f"  • True Negatives (TN):  {confusion_matrix['TN']:>5}  (Correctly ignored clean rows)")
    print(f"  • False Negatives (FN): {confusion_matrix['FN']:>5}  (Missed corrupted rows)")

    print(f"\n✨ Metrics:")
    p = confusion_matrix['precision']
    r = confusion_matrix['recall']
    f = confusion_matrix['f1_score']
    if confusion_matrix.get('metrics_defined', True) and p is not None and r is not None:
        print(f"  • Precision: {p*100:>5.1f}%  (TP / (TP + FP))")
        print(f"  • Recall:    {r*100:>5.1f}%  (TP / (TP + FN))")
        print(f"  • F1 Score:  {f*100:>5.1f}%" if f is not None else "  • F1 Score:    N/A")
    else:
        print(f"  • Precision:   N/A  (no corrupted rows — metric undefined)")
        print(f"  • Recall:      N/A  (no corrupted rows — metric undefined)")
        print(f"  • F1 Score:    N/A  (excluded from aggregate stats)")
    print(f"  • Accuracy:  {confusion_matrix['accuracy']*100:>5.1f}%  ((TP + TN) / Total)")


def generate_summary_report(results: List[Dict]):
    """Generate overall summary report"""
    print(f"\n{'='*70}")
    print(f"  OVERALL BENCHMARK SUMMARY")
    print(f"{'='*70}")

    total_datasets = len(results)
    total_duration = sum(r['pipeline_results']['duration_seconds'] for r in results)

    # Only include datasets where metrics are fully computable
    scored_results = [
        r for r in results
        if r['confusion_matrix'].get('metrics_defined', True)
        and r['confusion_matrix']['precision'] is not None
        and r['confusion_matrix']['recall'] is not None
    ]
    skipped = [r['dataset_name'] for r in results if r not in scored_results]

    print(f"\n📊 Test Summary:")
    print(f"  • Total Datasets Tested: {total_datasets}")
    if skipped:
        print(f"  • Excluded from F1 stats: {', '.join(skipped)} (no corrupted rows — metrics undefined)")
    print(f"  • Total Execution Time: {total_duration:.1f}s ({total_duration/60:.1f}m)")

    if scored_results:
        avg_precision = sum(r['confusion_matrix']['precision'] for r in scored_results) / len(scored_results)
        avg_recall = sum(r['confusion_matrix']['recall'] for r in scored_results) / len(scored_results)
        avg_f1 = sum(r['confusion_matrix']['f1_score'] for r in scored_results) / len(scored_results)

        print(f"\n📈 Average Metrics ({len(scored_results)} scored datasets):")
        print(f"  • Precision: {avg_precision*100:.1f}%")
        print(f"  • Recall:    {avg_recall*100:.1f}%")
        print(f"  • F1 Score:  {avg_f1*100:.1f}%")

        best = max(scored_results, key=lambda r: r['confusion_matrix']['f1_score'])
        worst = min(scored_results, key=lambda r: r['confusion_matrix']['f1_score'])

        print(f"\n🏆 Best Performing Dataset:")
        print(f"  • {best['dataset_name']} (F1: {best['confusion_matrix']['f1_score']*100:.1f}%)")

        print(f"\n⚠️  Worst Performing Dataset:")
        print(f"  • {worst['dataset_name']} (F1: {worst['confusion_matrix']['f1_score']*100:.1f}%)")
    else:
        print(f"\n📈 No scored datasets (all have undefined metrics)")

    # Recommendations — only for scored datasets with computable metrics
    recommendations = []
    for result in scored_results:
        cm = result['confusion_matrix']
        dataset_name = result['dataset_name']
        if cm['recall'] is not None and cm['recall'] < 0.80:
            recommendations.append(f"  • Improve detection for {dataset_name} (low recall: {cm['recall']*100:.1f}%)")
        if cm['precision'] is not None and cm['precision'] < 0.80:
            recommendations.append(f"  • Reduce false positives in {dataset_name} (low precision: {cm['precision']*100:.1f}%)")

    if recommendations:
        print(f"\n💡 Recommendations:")
        for rec in recommendations:
            print(rec)
    else:
        print(f"\n✅ No recommendations — all scored datasets meet thresholds")


def save_json_report(results: List[Dict], output_path: Path):
    """Save detailed results as JSON"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    test_run_id = f"bench-{timestamp}"

    report = {
        'test_run_id': test_run_id,
        'executed_at': datetime.now().isoformat(),
        'datasets_tested': len(results),
        'total_duration_seconds': sum(r['pipeline_results']['duration_seconds'] for r in results),
        'results': []
    }

    for result in results:
        report['results'].append({
            'dataset_name': result['dataset_name'],
            'corruption_type': result['metadata'].get('corruption_type'),
            'ground_truth': result['metadata'].get('ground_truth'),
            'pipeline_results': {
                'status': result['pipeline_results'].get('status'),
                'flags_created': len(result['pipeline_results'].get('flags', [])),
                'duration_seconds': result['pipeline_results'].get('duration_seconds')
            },
            'confusion_matrix': result['confusion_matrix'],
            'passed': (result['confusion_matrix']['f1_score'] >= 0.80
                      if result['confusion_matrix'].get('metrics_defined', True)
                         and result['confusion_matrix']['f1_score'] is not None
                      else None)
        })

    # Calculate summary — exclude datasets with undefined/None metrics
    scored = [
        r for r in results
        if r['confusion_matrix'].get('metrics_defined', True)
        and r['confusion_matrix']['precision'] is not None
        and r['confusion_matrix']['recall'] is not None
    ]
    report['summary'] = {
        'scored_datasets': len(scored),
        'skipped_datasets': len(results) - len(scored),
        'avg_precision': sum(r['confusion_matrix']['precision'] for r in scored) / len(scored) if scored else None,
        'avg_recall': sum(r['confusion_matrix']['recall'] for r in scored) / len(scored) if scored else None,
        'avg_f1': sum(r['confusion_matrix']['f1_score'] for r in scored) / len(scored) if scored else None,
        'all_passed': all(r['passed'] for r in report['results'] if r['passed'] is not None) if any(r['passed'] is not None for r in report['results']) else None
    }

    # Save to file
    output_file = output_path / f"{test_run_id}.json"
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"\n💾 JSON report saved to: {output_file}")
    return output_file


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("  ERGONOSIS AUDITING - AUTOMATED TESTING FRAMEWORK")
    print("="*70)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Create benchmark results directory
    BENCHMARK_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Discover datasets
    datasets = discover_datasets()

    if not datasets:
        print("\n❌ No datasets found. Please run generate_test_datasets.py first.")
        sys.exit(1)

    # Run tests on each dataset
    all_results = []

    for dataset_path in datasets:
        dataset_name = dataset_path.name

        # Load metadata
        metadata = load_metadata(dataset_path)

        # Run pipeline
        pipeline_results = run_pipeline_on_dataset(dataset_path)

        # Calculate confusion matrix
        confusion_matrix = calculate_confusion_matrix(
            pipeline_results.get('flags', []),
            metadata.get('ground_truth', {})
        )

        # Print results
        print_dataset_results(dataset_name, metadata, pipeline_results, confusion_matrix)

        # Store results
        all_results.append({
            'dataset_name': dataset_name,
            'metadata': metadata,
            'pipeline_results': pipeline_results,
            'confusion_matrix': confusion_matrix
        })

    # Generate summary report
    generate_summary_report(all_results)

    # Save JSON report
    save_json_report(all_results, BENCHMARK_RESULTS_DIR)

    print("\n" + "="*70)
    print("  ✅ TESTING COMPLETE")
    print("="*70)
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
