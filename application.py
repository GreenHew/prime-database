from primedb import endpoints
from flask import Flask, Response, request, jsonify, redirect, url_for, render_template
from flask_sslify import SSLify
import os

app = Flask(__name__, template_folder='templates', static_url_path='/static', subdomain_matching=True)
application = app

if os.environ.get('USE_SSL') == 'true':
    sslify = SSLify(app)


@app.route('/', methods=['GET', 'POST'])
def root():
    if request.method == 'POST':
        return render_template_from_input(request.form['select_choice'], request.form['n'])
    else:
        return render_template('root.html', prev_choice="nth prime number")


def render_template_from_input(search_type, text_input):
    if search_type != 'primes from n to m':
        try:
            n = int(text_input.replace(',', ''))
        except:
            return render_template('root.html', display="Invalid input.", prev_choice=search_type)
    if search_type == 'nth prime number':
        return nth_prime_template(n, search_type)
    elif search_type == 'primes up to n':
        return pi_n_template(n, search_type)
    elif search_type == 'primality test':
        return primality_template(n, search_type)
    elif search_type == 'primes from n to m':
        text = request.form['n']
        if ',' not in text:
            return render_template('root.html', display="Must contain two numbers seperated by a comma. E.g. 123, 45678", prev_choice=search_type)
        return primes_from_n_to_m_template(text, search_type)


def nth_prime_template(n, search_type):
    resp, status = endpoints.get_nth_prime_from_bin(n, '1e9', '1e6')
    if status != 200:
        return render_template('root.html', display=f"{resp['message']}", prev_choice=search_type)
    nth_prime = resp['nth_prime']
    return render_template('root.html', display=f'The {n:,} prime number is {nth_prime:,}', prev_choice=search_type)


def pi_n_template(n, search_type):
    resp, status = endpoints.get_prime_count_up_to_n(n, '1e9', '1e6')
    if status != 200:
        return render_template('root.html', display=f"{resp['message']}", prev_choice=search_type)
    count = resp['count']
    return render_template('root.html', display=f'There are {count:,} prime numbers up to and including {n:,}', prev_choice=search_type)


def primality_template(n, search_type):
    resp, status = endpoints.get_sequence_number_for_prime_n(n, '1e9', '1e6')
    if status != 200:
        return render_template('root.html', display=f"{resp['message']}", prev_choice=search_type)
    is_prime = resp['is_prime']
    if is_prime:
        sequence_number = resp['sequence_number']
        return render_template('root.html', display=f'{n:,} is the {sequence_number:,} prime', prev_choice=search_type)
    return render_template('root.html', display=f'{n:,} is not a prime number', prev_choice=search_type)


def primes_from_n_to_m_template(text, search_type):
    nm_range = text.split(',')
    try:
        n, m = int(nm_range[0]), int(nm_range[1])
    except:
        return render_template('root.html', display="Invalid input", prev_choice=search_type)
    resp, status = endpoints.get_prime_count_from_n_to_m(n, m, '1e9', '1e6')
    if status != 200:
        return render_template('root.html', display=f"{resp['message']}", prev_choice=search_type)
    count = resp['count']
    return render_template('root.html', display=f'There are {count:,} prime numbers from {n:,} to {m:,}', prev_choice=search_type)


@app.route('/health', methods=['GET'])
def health():
    return Response('Ok')


@app.route('/api', methods=['GET'])
def api():
    return render_template('api.html')


@app.route('/api/primes/nth/<int:n>/', methods=['GET'])
def get_nth_prime(n):
    resp, status = endpoints.get_nth_prime_from_bin(n, '1e9', '1e6')
    return jsonify(resp), status


@app.route('/api/primes/upto/<int:n>/', methods=['GET'])
def get_primes_upto_n(n):
    resp, status = endpoints.get_prime_count_up_to_n(n, '1e9', '1e6')
    return jsonify(resp), status


@app.route('/api/primes/between/<int:n>/<int:m>', methods=['GET'])
def get_primes_from_n_to_m(n, m):
    resp, status = endpoints.get_prime_count_from_n_to_m(n, m, '1e9', '1e6')
    return jsonify(resp), status


@app.route('/api/primes/isprime/<int:n>/', methods=['GET'])
def get_sequence_number_for_prime(n):
    resp, status = endpoints.get_sequence_number_for_prime_n(n, '1e9', '1e6')
    return jsonify(resp), status


if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=app.config.get("PORT", 7331),
            threaded=True)