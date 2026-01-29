# runner.py
import subprocess
import time
import sys
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class Runner:
    def __init__(self):
        self.process = None
        self.restart = False
        self.should_exit = False
        # Get the current directory where the runner is located
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        
    def start_process(self):
        """Starts the main process"""
        print("Starting main process...")
        # Build the full path to main.py
        main_path = os.path.join(self.current_dir, "main.py")
        print(f"Running: {main_path}")
        
        # Use the same Python interpreter running this script
        self.process = subprocess.Popen(
            [sys.executable, main_path],
            stdout=sys.stdout,
            stderr=sys.stderr,
            cwd=self.current_dir  # Set the working directory
        )
        
    def monitor(self):
        """Monitors the process and restarts it if necessary"""
        while not self.should_exit:
            # Check if the process is still running
            if self.process and self.process.poll() is not None:
                print("Main process terminated. Restarting...")
                self.start_process()
            
            # Restart if requested
            if self.restart and self.process:
                print("Restarting process due to changes...")
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                self.start_process()
                self.restart = False
                
            time.sleep(1)
            
    def file_change_handler(self):
        """Handles file changes"""
        class Handler(FileSystemEventHandler):
            def __init__(self, callback):
                self.callback = callback
                self.last_trigger = 0
                
            def on_modified(self, event):
                if not event.is_directory and event.src_path.endswith('.py'):
                    # Ignore changes in the runner itself
                    if os.path.basename(event.src_path) == 'runner.py':
                        return
                    current_time = time.time()
                    if current_time - self.last_trigger > 2:  # 2 second debounce
                        print(f"Change detected in {os.path.basename(event.src_path)}")
                        self.last_trigger = current_time
                        self.callback()
        
        observer = Observer()
        event_handler = Handler(lambda: setattr(self, 'restart', True))
        
        # Monitor all relevant directories
        directories = [self.current_dir, 
                      os.path.join(self.current_dir, 'analytics'),
                      os.path.join(self.current_dir, 'config'),
                      os.path.join(self.current_dir, 'database'),
                      os.path.join(self.current_dir, 'processing')]
        
        for directory in directories:
            if os.path.exists(directory):
                observer.schedule(event_handler, directory, recursive=True)
                print(f"Monitoring: {directory}")
        
        return observer

    def run(self):
        """Runs the runner"""
        print("Starting runner with hot reload...")
        print(f"Current directory: {self.current_dir}")
        print("Press Ctrl+C to exit")
        
        self.start_process()
        observer = self.file_change_handler()
        
        try:
            observer.start()
            self.monitor()
        except KeyboardInterrupt:
            print("\nStopping runner...")
            self.should_exit = True
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            observer.stop()
        finally:
            observer.join()
            print("Runner stopped")

if __name__ == "__main__":
    runner = Runner()
    runner.run()



    