from flask import Flask, request
app = Flask(__name__)

@app.route('/client/info', methods=['GET'])
def client_info():
    return {
        'content-type': 'application/json',
        'user_agent': str(request.user_agent)
    }

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000)