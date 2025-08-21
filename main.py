from nicegui import ui
from ui.dashboard import create_dashboard
from data_model import initialize_data
import concurrent.futures
import sys
import asyncio
import multiprocessing

def setup_thread_pool():
    """Setup thread pool for PyInstaller compatibility"""
    if getattr(sys, 'frozen', False):  # Running as PyInstaller bundle
        # Create explicit thread pool executor
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=6,  # Adjust based on your needs
            thread_name_prefix='AsyncIO'
        )
        # Set as default executor for the event loop
        loop = asyncio.get_event_loop()
        loop.set_default_executor(executor)
        return executor
    return None

# Call this early in your app
#executor = setup_thread_pool()

def main():
    if getattr(sys, 'frozen', False):
        multiprocessing.freeze_support()
        multiprocessing.set_start_method('spawn', force=True)
    executor = setup_thread_pool()
    # Initialize data
    initialize_data()
    
    # Create dashboard UI
    
    # Run the app
    ui.run(reload=False,title="ML Integration",reconnect_timeout=7000, storage_secret='my_secret_key', host="0.0.0.0", port=8000)

if __name__ in {"__main__", "__mp_main__"}:
    main()