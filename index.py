from flask import Flask, request, jsonify
from flask_cors import CORS
from TwoPhaseLocking import TwoPhaseLocking

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/')
def home():
    return 'Hello, World!!'

@app.route('/about')
def about():
    return 'About'

@app.route('/2pl', methods=['POST'])
def two_phase_locking_route():
    try:
        if request.method == 'POST':
            data = request.get_json()
            if data is not None and 'sequence' in data:
                sequence = data['sequence']
                tpl = TwoPhaseLocking(sequence)
                tpl.run()
                result = tpl.result_string()
                return jsonify({"result": result})
            else:
                return jsonify({"error": "Invalid data format"})
        else:
            return jsonify({"error": "Method not allowed"})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(debug=True)
