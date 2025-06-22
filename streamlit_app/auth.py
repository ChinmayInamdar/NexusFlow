# streamlit_app/auth.py
import streamlit as st
import bcrypt
import os
import sys

# Add project root to sys.path if not already there for src.config
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) # Assumes streamlit_app is at project_root/streamlit_app/
if project_root not in sys.path:
    sys.path.append(project_root)

from src.config import logger

# Hashed passwords:
# "admin" / "password123" -> b'$2b$12$Eix9xQ5Yf9R8qP3zT6n9jOMhV5mou1q5x7J2G/R9u2.Y.2q3E2s/2'
# "user" / "test" -> b'$2b$12$WZfG1vJtVNcYw5g8x7rR2OFRj5W9mBCP.x9.S0H.3n1X.kI5c/e9m'
USER_CREDENTIALS_HASHED = {
    "admin": b'$2b$12$Eix9xQ5Yf9R8qP3zT6n9jOMhV5mou1q5x7J2G/R9u2.Y.2q3E2s/2',
    "user": b'$2b$12$WZfG1vJtVNcYw5g8x7rR2OFRj5W9mBCP.x9.S0H.3n1X.kI5c/e9m'
}

# --- Sign Up (VERY BASIC - NOT FOR PRODUCTION - stores in plain text in session for demo) ---
# In a real app, this would write to a database after hashing the password.
if "temp_users" not in st.session_state:
    st.session_state.temp_users = {} # username: hashed_password

def basic_signup(username, password):
    if username in USER_CREDENTIALS_HASHED or username in st.session_state.temp_users:
        return False, "Username already exists."
    if not username or not password:
        return False, "Username and password cannot be empty."
    
    # Hash the password for storage (even for this demo, it's good practice)
    hashed_new_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    st.session_state.temp_users[username] = hashed_new_password
    logger.info(f"Temporary user '{username}' signed up.")
    return True, "Signup successful! You can now log in."

def check_hashed_password(username, password):
    """Checks if the provided password matches the stored hashed password."""
    password_bytes = password.encode('utf-8') if isinstance(password, str) else password
    
    if username in USER_CREDENTIALS_HASHED:
        hashed_pw = USER_CREDENTIALS_HASHED[username]
        return bcrypt.checkpw(password_bytes, hashed_pw)
    elif username in st.session_state.temp_users: # Check temp users
        hashed_pw = st.session_state.temp_users[username]
        return bcrypt.checkpw(password_bytes, hashed_pw)
    return False

def show_login_form(container):
    with container:
        st.markdown("""
            <style>
                /* Center the login form */
                div[data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlock"] {
                    display: flex;
                    flex-direction: column;
                    align-items: center; /* Horizontally center */
                    justify-content: center; /* Vertically center if the container has height */
                    width: 100%;
                }
                .login-form-container {
                    max-width: 400px; /* Max width of the form itself */
                    padding: 2rem;
                    background-color: #ffffff; /* White background for the form */
                    border-radius: 10px;
                    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                }
            </style>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([1, 1.5, 1]) # Adjust ratios to center
        with col2:
            with st.container(): # Apply styling to this container
                st.markdown("<div class='login-form-container'>", unsafe_allow_html=True)
                st.image("https://i.imgur.com/3g8aq0q.png", width=100, use_column_width='auto')
                st.subheader("🔑 NexusFlow Dashboard Login")
                
                login_tab, signup_tab = st.tabs(["Login", "Sign Up (Demo)"])

                with login_tab:
                    with st.form("login_form"):
                        username_input = st.text_input("Username", key="login_username_input_form", placeholder="admin or user")
                        password_input = st.text_input("Password", type="password", key="login_password_input_form", placeholder="password123 or test")
                        login_submitted = st.form_submit_button("Login", use_container_width=True, type="primary")
                        
                        if login_submitted:
                            if check_hashed_password(username_input, password_input):
                                st.session_state.authenticated = True
                                st.session_state.username = username_input
                                logger.info(f"User '{username_input}' authenticated successfully.")
                                st.rerun() # Rerun to hide login and show app
                            else:
                                st.error("Incorrect username or password.")
                                logger.warning(f"Failed login attempt for username: '{username_input}'")
                
                with signup_tab:
                    with st.form("signup_form"):
                        new_username = st.text_input("New Username", key="signup_username")
                        new_password = st.text_input("New Password", type="password", key="signup_password")
                        confirm_password = st.text_input("Confirm Password", type="password", key="signup_confirm_password")
                        signup_submitted = st.form_submit_button("Sign Up", use_container_width=True)

                        if signup_submitted:
                            if not new_username or not new_password:
                                st.warning("Username and password cannot be empty.")
                            elif new_password != confirm_password:
                                st.warning("Passwords do not match.")
                            else:
                                success, message = basic_signup(new_username, new_password)
                                if success:
                                    st.success(message)
                                else:
                                    st.error(message)
                st.markdown("</div>", unsafe_allow_html=True)


def authenticate_user_interface():
    """Handles the user authentication flow and UI display."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username = None

    if not st.session_state.authenticated:
        show_login_form(st.container()) # Show form in the main area
        return False 
    return True