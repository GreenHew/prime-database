from primedb import endpoints
from flask import Flask, Response, request, jsonify
from flask_sslify import SSLify
import os

app = Flask(__name__)
application = app

if os.environ.get('USE_SSL') == 'true':
    sslify = SSLify(app)


@app.route('/', methods=['GET'])
def root():
    return Response('Application Backend v{0}.'.format("0.1.0"))


@app.route('/health', methods=['GET'])
def health():
    return Response('Ok')


@app.route('/example_endpoint', methods=['POST'])
def example_endpoint():
    resp, status = endpoints.example_endpoint(request.json)
    return jsonify(resp), status

# TODO: get_nth_prime
# TODO: get_prime_count_from_n_to_m
# TODO: sequence_number_for_prime_n
# TODO: get_primes_from_n_to_m

if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config.get("PORT", 7331),
            threaded=True)
