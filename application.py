from primedb import endpoints
from flask import Flask, Response, request, jsonify, redirect, url_for, render_template
from flask_sslify import SSLify
import os

app = Flask(__name__, template_folder='templates', static_url_path='/static')
application = app

if os.environ.get('USE_SSL') == 'true':
    sslify = SSLify(app)


@app.route('/', methods=['GET', 'POST'])
def root():
    if request.method == 'POST':
        search_type = request.form['select_choice']
        if search_type == 'nth prime number':
            n =  int(request.form['n'])
            nth_prime = endpoints.get_nth_prime_from_bin(n, '1e9', '1e6')
            return render_template('nth_prime.html', index=n, prime=nth_prime)
        elif search_type == 'primes up to n':
            n = int(request.form['n'])
            count = endpoints.get_prime_count_up_to_n(n, '1e9', '1e6')
            return render_template('upto_n.html', index=n, prime=count)
        elif search_type == 'primes from n to m':
            n = int(request.form['n'])
            count = endpoints.get_prime_count_up_to_n(n, '1e9', '1e6')
            return render_template('upto_n.html', index=n, prime=count)
    else:
        return render_template('root.html')


@app.route('/health', methods=['GET'])
def health():
    return Response('Ok')


def get_nth_prime(n):
    nth_prime = endpoints.get_nth_prime(n)
    return render_template('nth_prime.html', content=nth_prime)


@app.route('/example_endpoint', methods=['GET', 'POST'])
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
