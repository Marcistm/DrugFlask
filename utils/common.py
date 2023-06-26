import base64
import hmac
import time


def generate_token(key, expire=3600):
    """
    :param key: 用户给定的用于生成token的key
    :param expire: token过期时间，默认1小时，单位为s
    :return: token:str
    """
    ts_str = str(time.time() + expire)
    ts_byte = ts_str.encode("utf-8")
    sha1_tshexstr = hmac.new(key.encode("utf-8"), ts_byte, 'sha1').hexdigest()
    token = ts_str + ':' + sha1_tshexstr
    b64_token = base64.urlsafe_b64encode(token.encode("utf-8"))
    return b64_token.decode("utf-8")