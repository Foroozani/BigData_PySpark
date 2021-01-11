from flask import Flask, render_template, request

app = Flask(__name__)

# Index page, no args
@app.route('/')
def index():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(port=5000, debug=True)
