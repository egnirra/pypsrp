# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import abc

import httpx
import logging
import typing

from urllib.parse import urlparse

from .._wsman import (
    BasicAuth,
    NegotiateAuth,
    WSManTransport,
)

log = logging.getLogger(__name__)


class _WSManConnectionBase(metaclass=abc.ABCMeta):
    """The WSManConnection contract.

    This is the WSManConnection contract that defines what is required for a WSMan IO class to be used by this library.
    """

    _IS_ASYNC = False

    def __init__(
        self,
        connection_uri: str,
        encryption: str = "auto",
        verify: typing.Union[str, bool] = True,
        connection_timeout: int = 30,
        read_timeout: int = 30,
        # TODO: reconnection settings
        # Proxy settings
        proxy: typing.Optional[str] = None,
        proxy_username: typing.Optional[str] = None,
        proxy_password: typing.Optional[str] = None,
        proxy_auth: typing.Optional[str] = None,
        proxy_service: typing.Optional[str] = "HTTP",
        proxy_hostname: typing.Optional[str] = None,
        auth: str = "negotiate",
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        # Cert auth
        certificate_pem: typing.Optional[str] = None,
        certificate_key_pem: typing.Optional[str] = None,
        certificate_password: typing.Optional[str] = None,
        # SPNEGO
        negotiate_service: str = "HTTP",
        negotiate_hostname: typing.Optional[str] = None,
        negotiate_delegate: bool = False,
        send_cbt: bool = True,
        # CredSSP
        credssp_allow_tlsv1: bool = False,
        credssp_require_kerberos: bool = False,
    ):
        self.connection_uri = urlparse(connection_uri)

        if encryption not in ["auto", "always", "never"]:
            raise ValueError("The encryption value '%s' must be auto, always, or never" % encryption)

        encrypt = {
            "auto": self.connection_uri.scheme == "http",
            "always": True,
            "never": False,
        }[encryption]

        # Default for 'Accept-Encoding' is 'gzip, default' which normally doesn't matter on vanilla WinRM but for
        # Exchange endpoints hosted on IIS they actually compress it with 1 of the 2 algorithms. By explicitly setting
        # identity we are telling the server not to transform (compress) the data using the HTTP methods which we don't
        # support. https://tools.ietf.org/html/rfc7231#section-5.3.4
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "Python PSRP Client",
        }
        ssl_context = httpx.create_ssl_context(verify=verify)

        auth = auth.lower()
        if auth == "basic":
            auth = BasicAuth(username, password)

        elif auth == "certificate":
            headers["Authorization"] = "http://schemas.dmtf.org/wbem/wsman/1/wsman/secprofile/https/mutual"
            ssl_context.load_cert_chain(
                certfile=certificate_pem, keyfile=certificate_key_pem, password=certificate_password
            )
            auth = None

        elif auth in ["credssp", "kerberos", "negotiate", "ntlm"]:
            auth = NegotiateAuth(
                credential=(username, password),
                protocol=auth,
                encrypt=encrypt,
                service=negotiate_service,
                hostname_override=negotiate_hostname,
                disable_cbt=not send_cbt,
                delegate=negotiate_delegate,
                credssp_allow_tlsv1=credssp_allow_tlsv1,
                credssp_require_kerberos=credssp_require_kerberos,
            )

        else:
            raise ValueError("Invalid auth specified")

        if encrypt and not auth.SUPPORTS_ENCRYPTION:
            raise ValueError("Cannot encrypt without auth encryption")

        proxy_auth = proxy_auth.lower() if proxy_auth else None
        if proxy_auth == "basic":
            proxy_auth = BasicAuth(proxy_username, proxy_password)

        elif proxy_auth in ["kerberos", "negotiate", "ntlm"]:
            proxy_auth = NegotiateAuth(
                credential=(proxy_username, proxy_password),
                protocol=proxy_auth,
                encrypt=False,
                service=proxy_service,
                hostname_override=proxy_hostname,
            )

        elif proxy_auth is None or proxy_auth == "none":
            proxy_auth = None

        else:
            raise ValueError("Invalid proxy_auth specified")

        timeout = httpx.Timeout(max(connection_timeout, read_timeout), connect=connection_timeout, read=read_timeout)
        transport = WSManTransport(
            self._IS_ASYNC,
            auth=auth,
            ssl_context=ssl_context,
            keepalive_expiry=60.0,
            proxy_url=proxy,
            proxy_auth=proxy_auth,
        )
        client_type = httpx.AsyncClient if self._IS_ASYNC else httpx.Client
        self._http = client_type(headers=headers, timeout=timeout, transport=transport)

    async def __aenter__(self):
        """Implements 'async with' for the WSMan connection."""
        await self.open()
        return self

    def __enter__(self):
        """Implements 'with' for the WSMan connection."""
        self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Implements the closing method for 'async with' for the WSMan connection."""
        await self.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Implements the closing method for 'with' for the WSMan connection."""
        self.close()

    @abc.abstractmethod
    def send(
        self,
        data: bytes,
    ) -> bytes:
        """Send WSMan data to the endpoint.

        The WSMan envelope is sent as a HTTP POST request to the endpoint specified. This method should deal with the
        encryption required for a request if it is necessary.

        Args:
            data: The WSMan envelope to send to the endpoint.

        Returns:
            bytes: The WSMan response.
        """
        pass

    @abc.abstractmethod
    def open(self):
        """Opens the WSMan connection.

        Opens the WSMan connection and sets up the connection for sending any WSMan envelopes.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """Closes the WSMan connection.

        Closes the WSMan connection and any sockets/connections that are in use.
        """
        pass


class AsyncWSManConnection(_WSManConnectionBase):
    _IS_ASYNC = True

    async def send(
        self,
        data: bytes,
    ) -> bytes:
        response = await self._http.post(
            self.connection_uri.geturl(),
            content=data,
            headers={
                "Content-Type": "application/soap+xml;charset=UTF-8",
            },
        )

        content = await response.aread()

        # A WSManFault has more information that the WSMan state machine can
        # handle with better context so we ignore those.
        if response.status_code != 200 and (not content or b"wsmanfault" not in content):
            response.raise_for_status()

        return content

    async def open(self):
        await self._http.__aenter__()

    async def close(self):
        await self._http.aclose()


class WSManConnection(_WSManConnectionBase):
    _IS_ASYNC = False

    def send(
        self,
        data: bytes,
    ) -> bytes:
        log.debug("WSMan Request", data.decode())
        response = self._http.post(
            self.connection_uri.geturl(),
            content=data,
            headers={
                "Content-Type": "application/soap+xml;charset=UTF-8",
            },
        )

        content = response.read()
        if content:
            log.debug("WSMan Response", content.decode())

        # A WSManFault has more information that the WSMan state machine can
        # handle with better context so we ignore those.
        if response.status_code != 200 and (not content or b"wsmanfault" not in content):
            response.raise_for_status()

        return content

    def open(self):
        self._http.__enter__()

    def close(self):
        self._http.close()
