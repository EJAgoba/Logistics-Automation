import os

import sys

import subprocess

def main():

    # When bundled, the app files live under _MEIPASS

    base = getattr(sys, "_MEIPASS", os.path.abspath("."))

    app_path = os.path.join(base, "streamlit_app.py")

    # Pick an available port

    port = os.environ.get("STREAMLIT_SERVER_PORT", "8501")

    # Launch streamlit properly

    cmd = [

        sys.executable, "-m", "streamlit", "run", app_path,

        "--server.port", port,

        "--server.headless", "true",

        "--browser.serverAddress", "localhost",

    ]

    subprocess.Popen(cmd)

if __name__ == "__main__":

    main()
 