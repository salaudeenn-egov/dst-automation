"""
gen_drive_token.py
Run once to generate drive_token.json for salaudeen.n@egov.global.
Opens a browser — sign in with the company Google account.
"""
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.dirname(__file__))   # D:\DST\automation
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"))

CLIENT_SECRETS = os.path.join(_ROOT, "drive_oauth_client.json")
TOKEN_PATH     = os.path.join(_ROOT, "drive_token.json")
SCOPES         = ["https://www.googleapis.com/auth/drive"]

flow  = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, SCOPES)
creds = flow.run_local_server(port=0)

import json
data = {
    "token":          creds.token,
    "refresh_token":  creds.refresh_token,
    "token_uri":      creds.token_uri,
    "client_id":      creds.client_id,
    "client_secret":  creds.client_secret,
    "scopes":         list(creds.scopes),
    "universe_domain": "googleapis.com",
    "account":        "",
    "expiry":         creds.expiry.isoformat() if creds.expiry else None,
}
json.dump(data, open(TOKEN_PATH, "w"), indent=2)
print(f"\nToken saved to {TOKEN_PATH}")
print("Sign-in account:", creds.id_token.get("email") if hasattr(creds, "id_token") and creds.id_token else "check above")
