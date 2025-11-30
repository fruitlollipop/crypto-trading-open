"""
StandX 认证模块

实现 StandX API 的 JWT 认证和请求签名功能
基于官方文档: https://docs.standx.com/standx-api/perps-auth
"""

import time
import uuid
import base64
import json
from typing import Dict, Optional, Any
from datetime import datetime

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.hazmat.backends import default_backend
    ED25519_AVAILABLE = True
except ImportError:
    ED25519_AVAILABLE = False
    try:
        # 尝试使用 PyNaCl 作为备选
        import nacl.signing
        import nacl.encoding
        NACL_AVAILABLE = True
    except ImportError:
        NACL_AVAILABLE = False


class StandXAuth:
    """StandX 认证类 - 处理 JWT token 获取和请求签名"""

    def __init__(self, logger=None):
        """
        初始化认证类

        Args:
            logger: 日志记录器
        """
        self.logger = logger
        self.base_url = "https://api.standx.com"
        self.perps_base_url = "https://perps.standx.com"
        
        # ed25519 密钥对
        self.ed25519_private_key = None
        self.ed25519_public_key = None
        self.request_id = None
        
        # JWT token
        self.jwt_token = None
        self.token_expires_at = None
        
        # 生成密钥对
        self._generate_key_pair()

    def _generate_key_pair(self):
        """生成 ed25519 密钥对"""
        if ED25519_AVAILABLE:
            # 使用 cryptography 库
            self.ed25519_private_key = ed25519.Ed25519PrivateKey.generate()
            self.ed25519_public_key = self.ed25519_private_key.public_key()
            
            # 获取原始字节
            private_key_bytes = self.ed25519_private_key.private_bytes_raw()
            public_key_bytes = self.ed25519_public_key.public_bytes_raw()
            
            # Base58 编码 public key 作为 requestId
            self.request_id = self._base58_encode(public_key_bytes)
            
        elif NACL_AVAILABLE:
            # 使用 PyNaCl 作为备选
            signing_key = nacl.signing.SigningKey.generate()
            self.ed25519_private_key = signing_key
            self.ed25519_public_key = signing_key.verify_key
            
            # Base58 编码 public key
            public_key_bytes = bytes(signing_key.verify_key)
            self.request_id = self._base58_encode(public_key_bytes)
        else:
            raise ImportError(
                "需要安装 ed25519 支持库。请运行: pip install cryptography 或 pip install pynacl"
            )

    def _base58_encode(self, data: bytes) -> str:
        """Base58 编码（简化实现）"""
        # 注意：这里使用 base64 作为简化实现，实际应该使用 base58
        # 生产环境应该使用 base58 库
        try:
            import base58
            return base58.b58encode(data).decode('utf-8')
        except ImportError:
            # 如果没有 base58 库，使用 base64url 作为临时替代
            encoded = base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')
            if self.logger:
                self.logger.warning("使用 base64url 替代 base58，建议安装 base58 库")
            return encoded

    async def prepare_signin(self, chain: str, wallet_address: str) -> str:
        """
        准备签名数据

        Args:
            chain: 区块链网络 ("bsc" 或 "solana")
            wallet_address: 钱包地址

        Returns:
            signedData JWT 字符串
        """
        import aiohttp

        url = f"{self.base_url}/v1/offchain/prepare-signin?chain={chain}"
        data = {
            "address": wallet_address,
            "requestId": self.request_id
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    result = await response.json()
                    
                    if result.get("success"):
                        return result.get("signedData")
                    else:
                        raise Exception(f"准备签名失败: {result}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"准备签名数据失败: {e}")
            raise

    def parse_jwt(self, token: str) -> Dict[str, Any]:
        """
        解析 JWT token（不验证签名）

        Args:
            token: JWT token 字符串

        Returns:
            解析后的 payload 字典
        """
        try:
            # JWT 格式: header.payload.signature
            parts = token.split('.')
            if len(parts) != 3:
                raise ValueError("无效的 JWT 格式")

            # 解码 payload (第二部分)
            payload_b64 = parts[1]
            # Base64 URL 解码
            payload_b64 = payload_b64.replace('-', '+').replace('_', '/')
            # 添加填充
            padding = len(payload_b64) % 4
            if padding:
                payload_b64 += '=' * (4 - padding)

            payload_bytes = base64.b64decode(payload_b64)
            payload = json.loads(payload_bytes.decode('utf-8'))

            return payload
        except Exception as e:
            if self.logger:
                self.logger.error(f"解析 JWT token 失败: {e}")
            raise

    async def login(
        self,
        chain: str,
        signature: str,
        signed_data: str,
        expires_seconds: int = 604800  # 默认 7 天
    ) -> Dict[str, Any]:
        """
        登录获取 JWT access token

        Args:
            chain: 区块链网络 ("bsc" 或 "solana")
            signature: 钱包签名
            signed_data: 从 prepare_signin 获取的 signedData
            expires_seconds: token 过期时间（秒），默认 7 天

        Returns:
            登录响应，包含 token、address、chain 等信息
        """
        import aiohttp

        url = f"{self.base_url}/v1/offchain/login?chain={chain}"
        data = {
            "signature": signature,
            "signedData": signed_data,
            "expiresSeconds": expires_seconds
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    result = await response.json()
                    
                    if "token" in result:
                        self.jwt_token = result["token"]
                        # 解析 token 获取过期时间
                        try:
                            payload = self.parse_jwt(self.jwt_token)
                            if "exp" in payload:
                                self.token_expires_at = datetime.fromtimestamp(payload["exp"])
                        except Exception:
                            # 如果解析失败，使用默认过期时间
                            self.token_expires_at = datetime.fromtimestamp(
                                time.time() + expires_seconds
                            )
                    
                    return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"登录失败: {e}")
            raise

    async def authenticate(
        self,
        chain: str,
        wallet_address: str,
        sign_message_func: callable
    ) -> Dict[str, Any]:
        """
        完整的认证流程

        Args:
            chain: 区块链网络 ("bsc" 或 "solana")
            wallet_address: 钱包地址
            sign_message_func: 签名函数，接受消息字符串，返回签名

        Returns:
            登录响应
        """
        # 1. 准备签名数据
        signed_data = await self.prepare_signin(chain, wallet_address)
        
        # 2. 解析 signedData 获取 message
        payload = self.parse_jwt(signed_data)
        message = payload.get("message")
        
        if not message:
            raise ValueError("signedData 中未找到 message 字段")
        
        # 3. 使用钱包签名消息
        signature = await sign_message_func(message) if callable(sign_message_func) else sign_message_func(message)
        
        # 4. 登录获取 token
        return await self.login(chain, signature, signed_data)

    def sign_request(
        self,
        payload: str,
        request_id: Optional[str] = None,
        timestamp: Optional[int] = None
    ) -> Dict[str, str]:
        """
        签名请求体

        Args:
            payload: JSON 字符串格式的请求体
            request_id: 请求 ID（UUID），如果为 None 则自动生成
            timestamp: 时间戳（毫秒），如果为 None 则使用当前时间

        Returns:
            包含签名头的字典
        """
        if request_id is None:
            request_id = str(uuid.uuid4())
        
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        
        version = "v1"
        
        # 构建签名消息: "{version},{id},{timestamp},{payload}"
        sign_msg = f"{version},{request_id},{timestamp},{payload}"
        
        # 使用 ed25519 私钥签名
        if ED25519_AVAILABLE:
            message_bytes = sign_msg.encode('utf-8')
            signature_bytes = self.ed25519_private_key.sign(message_bytes)
            signature = base64.b64encode(signature_bytes).decode('utf-8')
        elif NACL_AVAILABLE:
            message_bytes = sign_msg.encode('utf-8')
            signature_bytes = self.ed25519_private_key.sign(message_bytes)
            signature = base64.b64encode(signature_bytes.signature).decode('utf-8')
        else:
            raise ImportError("需要 ed25519 支持库")
        
        return {
            "x-request-sign-version": version,
            "x-request-id": request_id,
            "x-request-timestamp": str(timestamp),
            "x-request-signature": signature
        }

    def get_auth_headers(self, include_signature: bool = False, payload: Optional[str] = None) -> Dict[str, str]:
        """
        获取认证请求头

        Args:
            include_signature: 是否包含请求签名
            payload: 请求体（JSON 字符串），如果 include_signature 为 True 则必需

        Returns:
            认证请求头字典
        """
        headers = {}
        
        # JWT token 认证
        if self.jwt_token:
            headers["Authorization"] = f"Bearer {self.jwt_token}"
        
        # 请求签名
        if include_signature and payload:
            signature_headers = self.sign_request(payload)
            headers.update(signature_headers)
        
        return headers

    def is_token_valid(self) -> bool:
        """检查 token 是否仍然有效"""
        if not self.jwt_token or not self.token_expires_at:
            return False
        
        # 检查是否已过期（提前 5 分钟刷新）
        now = datetime.now()
        return now < (self.token_expires_at - datetime.fromtimestamp(0).replace(second=300))

    def get_verification_public_key(self) -> str:
        """
        获取 StandX 的验证公钥（用于验证 signedData）

        Returns:
            公钥字符串
        """
        import aiohttp
        import asyncio

        url = f"{self.base_url}/v1/offchain/certs"
        
        try:
            # 注意：这里使用同步请求，实际应该使用异步
            # 为了简化，这里返回 URL，实际使用时应该异步获取
            return url
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取验证公钥失败: {e}")
            raise

