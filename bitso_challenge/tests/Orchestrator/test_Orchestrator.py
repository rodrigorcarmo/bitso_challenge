import pytest
from unittest.mock import MagicMock, patch
from utils.Orchestrator.Orchestrator import Orchestrator

@pytest.fixture
def orchestrator():
    return Orchestrator()

def test_run(orchestrator):
    orchestrator.fetch_data = MagicMock()
    orchestrator.prepare_data_for_transform = MagicMock()
    orchestrator.transform_data = MagicMock()
    orchestrator.write_data = MagicMock()

    orchestrator.run()

    orchestrator.fetch_data.assert_called_once()
    orchestrator.prepare_data_for_transform.assert_called_once()
    orchestrator.transform_data.assert_called_once()
    orchestrator.write_data.assert_called_once()