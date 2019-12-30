import requests
from primedb.endpoints import example_endpoint

env_config = {
    "local": {
        "url": "http://0.0.0.0:7331/",
    },
    "prod": {
        "url": ""
    },
    "code": {
        'url': 'code://'
    }
}


def test_example_endpoint(env):
    route_name = "example_endpoint"
    body = {}
    url = env_config[env]['url'] + route_name
    print(url)
    if env == "code":
        resp, status = example_endpoint(body)
    else:
        resp = requests.post(url, json=body)
        resp, status = resp.text, resp.status_code

    print(status, resp)


def test_endpoints(env):
    test_example_endpoint(env)


if __name__ == '__main__':
    test_endpoints('code')
    # test_endpoints('local')
    # test_endpoints('prod')
