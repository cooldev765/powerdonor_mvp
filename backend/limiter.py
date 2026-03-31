"""
Shared rate limiter instance — imported by main.py (to attach to app)
and by any route that needs a limit decorator.
"""
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
