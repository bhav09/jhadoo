"""Command-line interface for jhadoo."""

import argparse
import os
import sys
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any

from .config import Config
from .core import CleanupEngine
from .scheduler import Scheduler
from .utils import bytes_to_human_readable


def show_dashboard(config: Config):
    """Display summary dashboard with trends and statistics."""
    log_file = config.get("logging", {}).get("log_file")
    
    if not os.path.exists(log_file):
        print("\n📊 Dashboard")
        print("="*60)
        print("No cleanup history found yet.")
        print("Run jhadoo to start tracking your space savings!")
        return
    
    try:
        with open(log_file, 'r') as f:
            reader = list(csv.DictReader(f))
        
        if not reader:
            print("\n📊 Dashboard")
            print("="*60)
            print("No cleanup history found yet.")
            return
        
        # Calculate statistics
        total_runs = len(reader)
        last_row = reader[-1]
        
        cumulative_total_mb = float(last_row['cumulative_total_mb'])
        cumulative_folders_mb = float(last_row['cumulative_folders_mb'])
        cumulative_bin_mb = float(last_row['cumulative_bin_mb'])
        
        # Get last 7 days of data
        now = datetime.now()
        recent_runs = []
        total_last_7_days = 0.0
        
        for row in reader:
            run_date = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            if (now - run_date).days <= 7:
                recent_runs.append(row)
                total_last_7_days += float(row['total_deleted_mb'])
        
        # Calculate average per run
        avg_per_run = cumulative_total_mb / total_runs if total_runs > 0 else 0
        
        # Trend analysis
        if len(reader) >= 2:
            prev_total = float(reader[-2]['cumulative_total_mb'])
            last_total = cumulative_total_mb
            trend = last_total - prev_total
            trend_indicator = "📈" if trend > 0 else "📉" if trend < 0 else "➡️"
        else:
            trend = 0
            trend_indicator = "➡️"
        
        # Display dashboard
        print("\n" + "="*60)
        print("📊 jhadoo Dashboard - Space Savings Summary")
        print("="*60)
        
        print(f"\n🎯 All-Time Statistics:")
        print(f"   Total space freed: {bytes_to_human_readable(cumulative_total_mb * 1024 * 1024)}")
        print(f"   • From folders: {bytes_to_human_readable(cumulative_folders_mb * 1024 * 1024)}")
        print(f"   • From bin: {bytes_to_human_readable(cumulative_bin_mb * 1024 * 1024)}")
        print(f"   Total cleanup runs: {total_runs}")
        print(f"   Average per run: {bytes_to_human_readable(avg_per_run * 1024 * 1024)}")
        
        print(f"\n📅 Last 7 Days:")
        print(f"   Cleanup runs: {len(recent_runs)}")
        print(f"   Space freed: {bytes_to_human_readable(total_last_7_days * 1024 * 1024)}")
        
        if len(recent_runs) > 0:
            print(f"\n📜 Recent Activity:")
            for row in recent_runs[-5:]:  # Show last 5 runs
                date = row['datetime']
                total = float(row['total_deleted_mb'])
                print(f"   • {date}: {bytes_to_human_readable(total * 1024 * 1024)}")
        
        print(f"\n{trend_indicator} Trend:")
        if trend > 0:
            print(f"   Last run freed {bytes_to_human_readable(trend * 1024 * 1024)}")
        else:
            print(f"   No significant change")
        
        # Predictions
        if total_runs >= 3:
            avg_days_between = _calculate_avg_days_between_runs(reader)
            if avg_days_between > 0:
                predicted_monthly = (30 / avg_days_between) * avg_per_run
                print(f"\n🔮 Predictions:")
                print(f"   You run cleanup every ~{avg_days_between:.1f} days")
                print(f"   Estimated monthly savings: {bytes_to_human_readable(predicted_monthly * 1024 * 1024)}")
        
        print(f"\n💡 Tips:")
        if cumulative_folders_mb > cumulative_bin_mb * 2:
            print("   • Most space is from old project folders - consider archiving instead of deleting")
        if len(recent_runs) == 0:
            print("   • No recent activity - run jhadoo to free up space!")
        if avg_per_run > 1000:  # > 1GB per run
            print("   • You're freeing significant space - consider running more frequently")
        
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error reading dashboard data: {e}")


def _calculate_avg_days_between_runs(runs: List[Dict[str, Any]]) -> float:
    """Calculate average days between cleanup runs."""
    if len(runs) < 2:
        return 0.0
    
    dates = []
    for row in runs:
        try:
            date = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            dates.append(date)
        except:
            continue
    
    if len(dates) < 2:
        return 0.0
    
    total_days = (dates[-1] - dates[0]).days
    num_intervals = len(dates) - 1
    
    return total_days / num_intervals if num_intervals > 0 else 0.0


def generate_sample_config(output_path: str):
    """Generate a sample configuration file."""
    config = Config()
    config.save_to_file(output_path)
    print(f"✅ Sample configuration saved to: {output_path}")
    print("\nEdit this file to customize your cleanup settings.")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="jhadoo - Smart cleanup tool for development environments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  jhadoo                    Run cleanup with default settings
  jhadoo --dry-run          Preview what would be deleted
  jhadoo --archive          Move files to archive instead of deleting
  jhadoo --restore          Restore items from last archive run
  jhadoo --git-check        Analyze Git repositories for health
  jhadoo --docker           Include Docker image cleanup
  jhadoo --dashboard        Show statistics and trends
  jhadoo --config my.json   Use custom configuration file
  jhadoo --generate-config  Create a sample config file
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        help='Path to custom configuration file (JSON)',
        metavar='FILE'
    )
    
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview deletions without actually deleting'
    )
    
    parser.add_argument(
        '--archive', '-a',
        action='store_true',
        help='Move to archive folder instead of permanent deletion'
    )
    
    parser.add_argument(
        '--dashboard', '-d',
        action='store_true',
        help='Show summary dashboard with statistics and trends'
    )
    
    parser.add_argument(
        '--generate-config',
        action='store_true',
        help='Generate a sample configuration file'
    )
    
    parser.add_argument(
        '--config-output',
        default='jhadoo_config.json',
        help='Output path for generated config (default: jhadoo_config.json)',
        metavar='FILE'
    )
    
    parser.add_argument(
        '--schedule',
        choices=['daily', 'weekly', 'monthly', 'hourly', 'custom'],
        help='Schedule automated cleanup'
    )
    
    parser.add_argument(
        '--cron',
        metavar='EXPR',
        help='Custom cron expression (e.g., "0 2 * * 0" for Sunday 2 AM)'
    )
    
    parser.add_argument(
        '--list-schedules',
        action='store_true',
        help='List all scheduled cleanup tasks'
    )
    
    parser.add_argument(
        '--remove-schedule',
        action='store_true',
        help='Remove all scheduled cleanup tasks'
    )
    
    parser.add_argument(
        '--restore',
        action='store_true',
        help='Restore items deleted in the last run (requires --archive to have been used)'
    )
    
    parser.add_argument(
        '--docker',
        action='store_true',
        help='Enable Docker image cleanup (unused > 60 days)'
    )
    
    parser.add_argument(
        '--git-check',
        action='store_true',
        help='Run Git repository health analysis'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='%(prog)s 1.0.0'
    )
    
    args = parser.parse_args()
    
    # Handle config generation
    if args.generate_config:
        generate_sample_config(args.config_output)
        return 0
    
    # Handle scheduling operations
    scheduler = Scheduler()
    
    if args.list_schedules:
        scheduler.list_schedules()
        return 0
    
    if args.remove_schedule:
        success = scheduler.remove_schedule()
        return 0 if success else 1
    
    if args.schedule or args.cron:
        frequency = args.cron if args.cron else args.schedule
        success = scheduler.schedule(
            frequency=frequency,
            config_path=args.config,
            dry_run=args.dry_run,
            archive=args.archive
        )
        return 0 if success else 1
    
    # Load configuration
    config = Config(args.config)
    
    # Init logging
    import logging
    logging.basicConfig(level=getattr(logging, config.get("logging", {}).get("level", "INFO")))
    
    # Handle restore operation
    if args.restore:
        from .restore import JobRestorer
        restorer = JobRestorer(config)
        restorer.restore_all()
        return 0
    
    # Handle dashboard
    if args.dashboard:
        show_dashboard(config)
        return 0
    
    # Update config with CLI flags
    if args.docker:
        config.set("docker", {"enabled": True})
        
    if args.git_check:
        config.set("git", {"enabled": True})
        # Disable other cleanups if only checking git? 
        # For now let's allow mixing, but maybe user wants JUST git check.
        # If user explicitly asks for git check, maybe we should prioritize it or run it standalone?
        # The current design runs it as part of CleanupEngine.run()
    
    # Run cleanup
    engine = CleanupEngine(
        config=config,
        dry_run=args.dry_run,
        archive_mode=args.archive
    )
    
    result = engine.run()
    
    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())


