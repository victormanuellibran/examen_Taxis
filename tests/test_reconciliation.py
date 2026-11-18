import pytest

from src.quality import reconcile


def test_reconciliation_correct():
    reconcile(10, 7, 3)
    reconcile(0, 0, 0)


@pytest.mark.parametrize("counts", [(10, 7, 2), (10, 7, 4), (0, 1, -1)])
def test_reconciliation_incorrect(counts):
    with pytest.raises(ValueError, match="Reconciliación incorrecta"):
        reconcile(*counts)
