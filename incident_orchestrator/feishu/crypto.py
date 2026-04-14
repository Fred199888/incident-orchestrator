"""飞书事件签名验证"""
import hashlib
import hmac


def verify_signature(
    timestamp: str, nonce: str, encrypt_key: str, body: str, signature: str
) -> bool:
    """验证飞书事件回调签名

    算法: SHA256(timestamp + nonce + encrypt_key + body)
    """
    if not encrypt_key:
        return True  # 未配置 encrypt_key 时跳过验证

    content = f"{timestamp}{nonce}{encrypt_key}{body}"
    computed = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return hmac.compare_digest(computed, signature)
