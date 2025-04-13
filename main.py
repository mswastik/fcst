from nicegui import ui
from ui.dashboard import create_dashboard
from models.data_model import initialize_data

def main():
    # Initialize data
    initialize_data()
    
    # Create dashboard UI
    create_dashboard()
    
    # Run the app
    ui.run(title="Product Dashboard")

if __name__ in {"__main__", "__mp_main__"}:
    main()