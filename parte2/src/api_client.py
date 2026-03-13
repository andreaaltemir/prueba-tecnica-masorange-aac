import requests

class APIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
    
    def _request(self, endpoint: str, method: str = "GET", headers: dict = None, data: dict = None) -> dict | list:
        request_url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.request(method.upper(), request_url, headers = headers, json = data)
        response.raise_for_status()
        return response.json()
    
    def download_data(self, endpoint: str) -> dict | list:
        return self._request(endpoint, "GET")