#!/usr/bin/env python3
"""
SSO Configuration Test Script
Test if Microsoft OAuth2 environment variables are properly configured
"""

import os
import sys

def test_oauth_config():
    """Test Microsoft OAuth2 configuration"""

    print("🔍 Testing Microsoft OAuth2 Configuration")
    print("=" * 50)

    # Check environment variables
    client_id = os.getenv('MICROSOFT_CLIENT_ID')
    client_secret = os.getenv('MICROSOFT_CLIENT_SECRET')
    tenant_id = os.getenv('MICROSOFT_TENANT_ID', 'common')

    print(f"✅ MICROSOFT_CLIENT_ID: {'✅ Set' if client_id else '❌ Not set'}")
    print(f"✅ MICROSOFT_CLIENT_SECRET: {'✅ Set' if client_secret else '❌ Not set'}")
    print(f"✅ MICROSOFT_TENANT_ID: {tenant_id} {'✅ Set' if tenant_id != 'common' else '⚠️  Using default (common)'}")

    # Check if values are placeholders
    if client_id == 'your-client-id' or client_id == 'your-client-id-here':
        print("❌ MICROSOFT_CLIENT_ID is still set to placeholder value!")
        return False

    if client_secret == 'your-client-secret' or client_secret == 'your-client-secret-here':
        print("❌ MICROSOFT_CLIENT_SECRET is still set to placeholder value!")
        return False

    # Test OAuth URL construction
    if client_id and client_secret:
        oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
        oauth_url += f"?client_id={client_id}"
        oauth_url += "&response_type=code"
        oauth_url += "&redirect_uri=http://localhost:8000/auth/callback"
        oauth_url += "&scope=openid%20profile%20email"

        print(f"\n✅ OAuth URL would be: {oauth_url[:100]}...")
        print("✅ Configuration appears valid!")

        return True
    else:
        print("\n❌ Configuration incomplete. Please set all required environment variables.")
        return False

def main():
    print("🚀 Company Forecasting App - SSO Setup Test")
    print("=" * 50)

    success = test_oauth_config()

    if success:
        print("\n🎉 SSO configuration is ready!")
        print("You can now run the app with: python main.py")
    else:
        print("\n📋 To configure SSO:")
        print("1. Go to Azure Portal: https://portal.azure.com")
        print("2. Navigate to Azure Active Directory > App registrations")
        print("3. Create a new app registration or use existing one")
        print("4. Copy the Application (client) ID")
        print("5. Create a client secret")
        print("6. Copy the Tenant ID from Azure AD overview")
        print("7. Set environment variables:")
        print("   set MICROSOFT_CLIENT_ID=your-actual-client-id")
        print("   set MICROSOFT_CLIENT_SECRET=your-actual-client-secret")
        print("   set MICROSOFT_TENANT_ID=your-tenant-id")
        print("8. Or create a .env file with these values")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
