from flask import Blueprint, render_template, redirect, url_for, flash, request, session
from functools import wraps

# Create auth blueprint
auth = Blueprint('auth', __name__)

# Hardcoded credentials (for demo purposes only)
ADMIN_CREDENTIALS = {
    'username': 'admin',
    'password': 'admin123'
}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username == ADMIN_CREDENTIALS['username'] and password == ADMIN_CREDENTIALS['password']:
            session['logged_in'] = True
            session['username'] = username
            next_page = request.args.get('next')
            return redirect(next_page or url_for('main.index'))
        else:
            flash('Invalid username or password', 'error')
    
    return render_template('login.html')

@auth.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('username', None)
    return redirect(url_for('auth.login'))
