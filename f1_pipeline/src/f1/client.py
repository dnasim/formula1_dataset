from typing import Any, Dict, Optional, Callable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.jolpi.ca/ergast/f1" 

class F1Client:
    def __init__(self, base_url: str = BASE_URL, timeout_sec: int = 15):
        
        self.base_url = base_url.rstrip("/")
        
        self.timeout_sec = timeout_sec
        
        self.session = requests.Session()
        
        self.session.headers.update({"User-Agent": "f1-client/0.1"})
        
        retries = Retry(total=3, backoff_factor=0.5,
                        status_forcelist=(429,500,502,503,504),
                        allowed_methods={"GET"})
                        
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get(self, *segments: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        
        path = "/".join(s.strip("/") for s in segments if s)
        
        url = f"{self.base_url}/{path}/"
        
        r = self.session.get(url, params=params or {}, timeout=self.timeout_sec)
        
        r.raise_for_status()
        
        return r.json()