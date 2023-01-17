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
        return render_template_from_input(request.form['select_choice'], request.form['n'])
    else:
        return render_template('root.html')


def render_template_from_input(search_type, text_input):
    if search_type != 'primes from n to m':
        try:
            n = int(text_input.replace(',', ''))
        except:
            return render_template('root.html', display="Invalid input.")
    if search_type == 'nth prime number':
        return nth_prime_template(n)
    elif search_type == 'primes up to n':
        return pi_n_template(n)
    elif search_type == 'primality test':
        return primality_template(n)
    elif search_type == 'primes from n to m':
        text = request.form['n']
        if ',' not in text:
            return render_template('root.html', display="Must contain two numbers seperated by a comma. E.g. 123, 45678")
        nm_range = text.split(',')
        try:
            n, m = int(nm_range[0]), int(nm_range[1])
        except:
            return render_template('root.html', display="Number out of range.")
        return primes_from_n_to_m_template(n, m)


def nth_prime_template(n):
    nth_prime = endpoints.get_nth_prime_from_bin(n, '1e9', '1e6')
    if nth_prime == -1:
        return render_template('root.html', display="Number out of range.")
    return render_template('root.html', display=f'The {n:,} prime number is {nth_prime:,}')


def pi_n_template(n):
    count = endpoints.get_prime_count_up_to_n(n, '1e9', '1e6')
    if count == -1:
        return render_template('root.html', display="Number out of range.")
    return render_template('root.html', display=f'There are {count:,} prime numbers up to and including {n:,}')


def primality_template(n):
    sequence_number = endpoints.get_sequence_number_for_prime_n(n, '1e9', '1e6')
    if sequence_number == -1:
        return render_template('root.html', display=f'{n:,} is not a prime number.')
    if sequence_number == -2:
        print(n)
        return render_template('root.html', display="Number out of range.")
    return render_template('root.html', display=f'{n:,} is the {sequence_number:,} prime.')


def primes_from_n_to_m_template(n, m):

    count = endpoints.get_prime_count_from_n_to_m(n, m, '1e9', '1e6')
    if count == -1:
        return render_template('root.html', display="Number out of range.")
    return render_template('root.html', display=f'There are {count:,} prime numbers from {n:,} to {m:,}.')


@app.route('/health', methods=['GET'])
def health():
    return Response('Ok')


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
