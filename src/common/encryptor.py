import os

from cryptography.fernet import Fernet

key = 'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o='

def gen_key() -> bytes:
    return Fernet.generate_key()


def encrypt(message: bytes, key: bytes) -> bytes:
    return Fernet(key).encrypt(message)


def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)


if __name__ == '__main__':
    p = encrypt('manager@2023'.encode(), key.encode())
    print(p.decode("utf-8") )

    # key = os.getenv('FERNET_KEY', 'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o=')

    print(decrypt('gAAAAABl2Az_mxZDxnzzka29F9vD848YVpphNavOZxfaAIqrx3POdLVpRin4i-67mHWeYNIJ4SHG-BCzuo2TET-NQ7r-7rDGpQ=='.encode(),
                  'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o='.encode()))
