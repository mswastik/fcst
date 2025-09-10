"""
FCST Application Main Entry Point
Enhanced with Database Connection Pooling and Session Management
"""
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

from nicegui import ui, run #, app
from core.state_manager import initialize_global_state
# from core.auth_service import auth_service, AuthMiddleware
# from fastapi import FastAPI
from ui.dashboard import create_dashboard
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

def main():
    if getattr(sys, 'frozen', False):
        multiprocessing.freeze_support()
        multiprocessing.set_start_method('spawn', force=True)
    executor = setup_thread_pool()
    initialize_global_state()
    '''
    # Add authentication middleware to NiceGUI
    from starlette.middleware.base import BaseHTTPMiddleware
    from auth_service import AuthMiddleware

    # Add middleware using NiceGUI's middleware system
    app.add_middleware(AuthMiddleware)

    # Add authentication routes using NiceGUI's routing system
    @app.get('/auth/login')
    async def auth_login():
        """Initiate Microsoft OAuth2 login"""
        from auth_service import auth_service
        # Create a redirect to the Microsoft OAuth login
        redirect_url = f"https://login.microsoftonline.com/{auth_service.tenant_id}/oauth2/v2.0/authorize?client_id={auth_service.client_id}&response_type=code&redirect_uri=http://localhost:8000/auth/callback&scope=openid%20profile%20email"
        from fastapi.responses import RedirectResponse
        return RedirectResponse(redirect_url)

    @app.get('/auth/callback')
    async def auth_callback(code: str = None, state: str = None, error: str = None):
        """Handle OAuth2 callback"""
        from auth_service import auth_service

        if error:
            ui.navigate.to('/login')
            ui.notify(f'Authentication error: {error}', type='negative')
            return {"error": error}

        if code:
            # Exchange code for token
            import requests

            token_url = f"https://login.microsoftonline.com/{auth_service.tenant_id}/oauth2/v2.0/token"
            token_data = {
                'client_id': auth_service.client_id,
                'client_secret': auth_service.client_secret,
                'code': code,
                'grant_type': 'authorization_code',
                'redirect_uri': 'http://localhost:8000/auth/callback'
            }

            try:
                token_response = requests.post(token_url, data=token_data)
                token_json = token_response.json()

                if 'access_token' in token_json:
                    # Get user info
                    user_url = "https://graph.microsoft.com/v1.0/me"
                    headers = {'Authorization': f'Bearer {token_json["access_token"]}'}
                    user_response = requests.get(user_url, headers=headers)
                    user_info = user_response.json()

                    # Store user information
                    app.storage.user.update({
                        'authenticated': True,
                        'username': user_info.get('displayName', user_info.get('userPrincipalName', 'Unknown')),
                        'email': user_info.get('userPrincipalName', user_info.get('mail')),
                        'microsoft_id': user_info.get('id'),
                        'token': token_json
                    })

                    ui.navigate.to('/')
                    ui.notify('Login successful!', type='positive')
                    return {"message": "Login successful", "user": user_info}
                else:
                    ui.navigate.to('/login')
                    ui.notify('Authentication failed', type='negative')
                    return {"error": "Authentication failed"}

            except Exception as e:
                ui.navigate.to('/login')
                ui.notify(f'Authentication error: {str(e)}', type='negative')
                return {"error": str(e)}
    '''
    ui.run(reload=True,title="ML Integration",reconnect_timeout=7000, storage_secret='my_secret_key', host="0.0.0.0", port=8000)

if __name__ in {"__main__", "__mp_main__"}:
    main()