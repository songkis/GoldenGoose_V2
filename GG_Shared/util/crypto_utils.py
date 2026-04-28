from cryptography.fernet import Fernet

SECRET_KEY = b"mFt09RHW8pQ7s3tRDrvh7fdwXPCbyYvNbXUtXCWtZqk="
cipher_suite = Fernet(SECRET_KEY)

def generate_key():
    return Fernet.generate_key()

def encrypt_data(data):
    """암호화 함수"""
    if data:
        return cipher_suite.encrypt(data.encode()).decode()
    return data

def decrypt_data(data):
    """복호화 함수"""
    if data:
        return cipher_suite.decrypt(data.encode()).decode()
    return data

def is_encrypted(data):
    """암호화된 데이터인지 확인"""
    try:
        if isinstance(data, str) and data.startswith("gAAAAA"):
            return True
        return False
    except Exception:
        return False
