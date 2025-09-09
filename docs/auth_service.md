# Authentication Service API Documentation

The `auth_service.py` module provides Microsoft OAuth2 Single Sign-On (SSO) authentication for the FCST application, enabling secure user authentication through Microsoft Azure Active Directory.

## Classes

### AuthService

Main authentication service class that handles Microsoft OAuth2 authentication flow.

```python
class AuthService:
    """Handles Microsoft OAuth2 authentication"""
```

#### Initialization

```python
def __init__(self, app_instance=None):
    """Initialize AuthService with OAuth configuration."""
```

**Environment Variables Required:**
- `MICROSOFT_CLIENT_ID`: Azure AD application client ID
- `MICROSOFT_CLIENT_SECRET`: Azure AD application client secret
- `MICROSOFT_TENANT_ID`: Azure AD tenant ID (or 'common' for multi-tenant)

#### Core Methods

##### `is_authenticated(request: Request) -> bool`

Checks if the current user is authenticated.

```python
def is_authenticated(self, request: Request) -> bool:
    """Check if user is authenticated"""
```

**Parameters:**
- `request` (Request): FastAPI request object

**Returns:**
- `bool`: True if user is authenticated, False otherwise

##### `get_user_info() -> Dict`

Retrieves current user information from session storage.

```python
def get_user_info(self) -> Dict:
    """Get current user information"""
```

**Returns:**
- `Dict`: User information including:
  - `authenticated`: Authentication status
  - `username`: User's display name
  - `email`: User's email address
  - `user_id`: Microsoft user ID

##### `logout() -> None`

Clears the current user session.

```python
def logout(self) -> None:
    """Clear user session"""
```

##### `initiate_login(request: Request) -> RedirectResponse`

Initiates the Microsoft OAuth2 login flow.

```python
async def initiate_login(self, request: Request) -> RedirectResponse:
    """Initiate Microsoft OAuth2 login flow"""
```

**Parameters:**
- `request` (Request): FastAPI request object

**Returns:**
- `RedirectResponse`: Redirect to Microsoft OAuth authorization endpoint

## Usage Examples

### Basic Authentication Setup

```python
from core.auth_service import auth_service

# Check authentication status
if auth_service.is_authenticated(request):
    user_info = auth_service.get_user_info()
    print(f"Welcome, {user_info['username']}")
else:
    # Redirect to login
    return await auth_service.initiate_login(request)
```

### Integration with UI Components

```python
from nicegui import ui
from core.auth_service import auth_service

def create_auth_header():
    """Create authentication header component."""
    with ui.header():
        user_info = auth_service.get_user_info()

        if user_info.get('authenticated'):
            ui.label(f"Welcome, {user_info['username']}")
            ui.button('Logout', on_click=lambda: auth_service.logout())
        else:
            ui.button('Login', on_click=lambda: ui.navigate.to('/login'))
```

### FastAPI Route Integration

```python
from fastapi import APIRouter, Request, Depends
from core.auth_service import auth_service

router = APIRouter()

@router.get('/protected-endpoint')
async def protected_endpoint(request: Request):
    """Protected endpoint that requires authentication."""

    if not auth_service.is_authenticated(request):
        return {"error": "Authentication required"}

    user_info = auth_service.get_user_info()
    return {"message": f"Hello, {user_info['username']}"}
```

## Configuration

### Azure AD Application Setup

1. **Create Azure AD Application:**
   - Go to Azure Portal → Azure Active Directory → App registrations
   - Click "New registration"
   - Enter application name and redirect URI: `http://localhost:8000/auth/callback`

2. **Configure Authentication:**
   - In app registration → Authentication
   - Add redirect URI for your application
   - Enable "Access tokens" and "ID tokens"

3. **Get Application Credentials:**
   - Client ID: Found in app registration overview
   - Client Secret: Create in Certificates & secrets
   - Tenant ID: Found in Azure AD overview

### Environment Variables

Create a `.env` file in the project root:

```env
# Microsoft OAuth2 Configuration
MICROSOFT_CLIENT_ID=your-client-id-here
MICROSOFT_CLIENT_SECRET=your-client-secret-here
MICROSOFT_TENANT_ID=your-tenant-id-here
```

### Application Integration

```python
# In main.py
from core.auth_service import auth_service, AuthMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# Add authentication middleware
app.add_middleware(AuthMiddleware)

# Add authentication routes
@app.get('/auth/login')
async def auth_login(request: Request):
    """Initiate login flow."""
    return await auth_service.initiate_login(request)

@app.get('/auth/callback')
async def auth_callback(request: Request, code: str = None):
    """Handle OAuth callback."""
    try:
        # Process the authorization code
        token = await auth_service.oauth.microsoft.authorize_access_token(request)

        # Get user info from Microsoft Graph
        user_info = await auth_service.get_user_profile(token['access_token'])

        # Store user session
        app.storage.user.update({
            'authenticated': True,
            'username': user_info['displayName'],
            'email': user_info['mail'],
            'user_id': user_info['id']
        })

        return RedirectResponse('/')

    except Exception as e:
        return {"error": str(e)}
```

## Security Considerations

### Token Management
- Access tokens are stored in session storage
- Tokens expire according to Azure AD policy
- Automatic token refresh is handled by AuthLib

### Session Security
- User sessions are managed through NiceGUI's storage system
- Session data is encrypted using the application's storage secret
- Sessions expire when the browser is closed

### HTTPS Requirement
- OAuth2 requires HTTPS in production
- Configure SSL certificates for production deployment
- Use secure cookie settings for session management

## Error Handling

### Common Authentication Errors

#### Invalid Client Configuration
```python
try:
    return await auth_service.initiate_login(request)
except Exception as e:
    print(f"Authentication error: {e}")
    # Handle configuration errors
```

#### Token Expiration
```python
if not auth_service.is_authenticated(request):
    # Token expired, redirect to login
    return RedirectResponse('/auth/login')
```

#### Network Issues
```python
try:
    user_info = await auth_service.get_user_profile(token)
except Exception as e:
    # Handle network/API errors
    auth_service.logout()  # Clear invalid session
```

## Troubleshooting

### Debug Authentication Issues

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable OAuth debugging
auth_service.oauth.microsoft.client_kwargs['verify'] = False  # For development only
```

### Common Issues

1. **"Invalid client" error:**
   - Verify MICROSOFT_CLIENT_ID and MICROSOFT_CLIENT_SECRET
   - Check Azure AD application configuration

2. **"Redirect URI mismatch":**
   - Ensure redirect URI in Azure AD matches your callback URL
   - Include port number if not using default (80/443)

3. **"Token expired":**
   - Implement token refresh logic
   - Clear expired sessions automatically

4. **Session persistence issues:**
   - Check NiceGUI storage configuration
   - Verify storage_secret is set in ui.run()

## Integration Examples

### Complete FastAPI Integration

```python
from fastapi import FastAPI, Request, Depends
from fastapi.responses import RedirectResponse
from core.auth_service import auth_service
import uvicorn

app = FastAPI()

@app.get('/')
async def home(request: Request):
    if auth_service.is_authenticated(request):
        user_info = auth_service.get_user_info()
        return {"message": f"Welcome {user_info['username']}"}
    else:
        return RedirectResponse('/auth/login')

@app.get('/auth/login')
async def login():
    return await auth_service.initiate_login(request)

@app.get('/auth/callback')
async def callback(request: Request, code: str = None):
    # Handle OAuth callback
    token = await auth_service.oauth.microsoft.authorize_access_token(request)
    user_info = await auth_service.get_user_profile(token['access_token'])

    # Store session
    request.session['user'] = user_info
    return RedirectResponse('/')

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
```

This authentication system provides secure, enterprise-grade user authentication through Microsoft Azure Active Directory, enabling seamless integration with existing Microsoft 365 environments.
