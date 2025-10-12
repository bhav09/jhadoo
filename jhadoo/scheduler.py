"""Built-in scheduler for automated cleanup tasks."""

import sys
import platform
import subprocess
from typing import Optional
import shutil


class Scheduler:
    """Cross-platform scheduler for jhadoo."""
    
    def __init__(self):
        self.system = platform.system().lower()
        self.jhadoo_path = self._get_jhadoo_path()
    
    # def _get_jhadoo_path(self) -> str:
    #     """Get path to jhadoo executable."""
    #     result = subprocess.run(['which', 'jhadoo'], capture_output=True, text=True)
    #     return result.stdout.strip() if result.returncode == 0 else f"{sys.executable} -m jhadoo"

    def _get_jhadoo_path(self):
        """Return path to jhadoo executable in a cross-platform way."""
        jhadoo_path = shutil.which("jhadoo")
        if jhadoo_path:
            return jhadoo_path.strip()
        else:
            return f'"{sys.executable}" -m jhadoo'  # Fall back to running the module directly via Python
    
    def schedule(self, frequency: str, config_path: Optional[str] = None, 
                 dry_run: bool = False, archive: bool = False) -> bool:
        """Schedule jhadoo to run automatically."""
        cmd = self.jhadoo_path
        if config_path:
            cmd += f" --config {config_path}"
        if dry_run:
            cmd += " --dry-run"
        if archive:
            cmd += " --archive"
        
        cron_expr = self._parse_frequency(frequency)
        
        if self.system in ["darwin", "linux"]:
            return self._schedule_cron(cron_expr, cmd)
        elif self.system == "windows":
            return self._schedule_windows(frequency, cmd)
        
        print(f"❌ Unsupported OS: {self.system}")
        return False
    
    def _parse_frequency(self, frequency: str) -> str:
        """Convert simple frequency to cron expression."""
        presets = {
            'daily': '0 2 * * *', 'weekly': '0 2 * * 0', 'monthly': '0 2 1 * *',
            'hourly': '0 * * * *', 'twice-daily': '0 2,14 * * *'
        }
        return presets.get(frequency.lower(), frequency)
    
    def _schedule_cron(self, cron_expr: str, command: str) -> bool:
        """Schedule using cron (Unix/Linux/macOS)."""
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            existing = result.stdout if result.returncode == 0 else ""
            
            if 'jhadoo' in existing:
                print("⚠️  Updating existing schedule...")
                existing = '\n'.join([l for l in existing.split('\n') if 'jhadoo' not in l.lower()])
            
            new_crontab = existing.strip() + f"\n{cron_expr} {command}\n"
            process = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, text=True)
            process.communicate(input=new_crontab)
            
            if process.returncode == 0:
                print(f"✅ Scheduled: {cron_expr}")
                return True
            return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def _schedule_windows(self, frequency: str, command: str) -> bool:
        """Schedule using Task Scheduler (Windows)."""
        try:
            freq_map = {
                'daily': '/SC DAILY /ST 02:00', 'weekly': '/SC WEEKLY /D SUN /ST 02:00',
                'monthly': '/SC MONTHLY /D 1 /ST 02:00', 'hourly': '/SC HOURLY'
            }
            schedule = freq_map.get(frequency.lower(), '/SC DAILY /ST 02:00')
            
            cmd_parts = ['schtasks', '/CREATE', '/TN', 'jhadooCleanup',
                        '/TR', command, '/F'] + schedule.split()
            
            result = subprocess.run(cmd_parts, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Scheduled: {frequency}")
                return True
            return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def list_schedules(self):
        """List all scheduled jhadoo tasks."""
        print("\n📅 Scheduled Tasks:")
        print("=" * 60)
        
        try:
            if self.system in ["darwin", "linux"]:
                result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
                if result.returncode == 0:
                    lines = [l for l in result.stdout.split('\n') 
                            if 'jhadoo' in l.lower() and not l.startswith('#')]
                    print('\n'.join(f"  • {l}" for l in lines) if lines else "  None found")
            elif self.system == "windows":
                result = subprocess.run(['schtasks', '/Query', '/TN', 'jhadooCleanup'],
                                      capture_output=True, text=True)
                print(result.stdout if result.returncode == 0 else "  None found")
        except Exception as e:
            print(f"  Error: {e}")
    
    def remove_schedule(self) -> bool:
        """Remove all jhadoo scheduled tasks."""
        print("\n🗑️  Removing schedules...")
        try:
            if self.system in ["darwin", "linux"]:
                result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
                if result.returncode == 0:
                    lines = [l for l in result.stdout.split('\n') if 'jhadoo' not in l.lower()]
                    process = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, text=True)
                    process.communicate(input='\n'.join(lines))
                print("✅ Removed")
                return True
            elif self.system == "windows":
                subprocess.run(['schtasks', '/Delete', '/TN', 'jhadooCleanup', '/F'],
                             capture_output=True)
                print("✅ Removed")
                return True
        except Exception as e:
            print(f"❌ Error: {e}")
        return False

