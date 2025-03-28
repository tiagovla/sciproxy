import pytest
from unittest.mock import AsyncMock, patch
from sciproxy.utils import get_redirect_url


class MockedSession:
    """Context manager to mock aiohttp.ClientSession."""

    def __init__(self, response_url, response_status):
        self.response_url = response_url
        self.response_status = response_status

    def __enter__(self):
        self.patcher = patch("aiohttp.ClientSession")
        self.MockClientSession = self.patcher.start()
        self.mock_session = self.MockClientSession.return_value
        self.mock_response = AsyncMock()
        self.mock_response.url = self.response_url
        self.mock_response.status = self.response_status
        self.mock_session.head.return_value.__aenter__.return_value = self.mock_response
        return self.mock_session

    def __exit__(self, exc_type, exc_value, traceback):
        _, _, _ = exc_type, exc_value, traceback
        self.patcher.stop()


@pytest.mark.asyncio
async def test_get_redirect_url():
    with MockedSession(
        response_url="https://example.com/redirect", response_status=418
    ) as s:
        redirect_url = await get_redirect_url("10.1000/xyz123", s)
        assert redirect_url == "https://example.com/redirect"


@pytest.mark.asyncio
async def test_get_redirect_url_wrong_doi():
    doi = "10.1000/mocked_doi"
    with MockedSession(response_url=f"https://doi.org/{doi}", response_status=404) as s:
        redirect_url = await get_redirect_url(doi, s)
        assert redirect_url == f"https://doi.org/{doi}"
