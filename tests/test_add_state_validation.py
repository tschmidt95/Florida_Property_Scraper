from scripts import add_state


def test_add_state_validation():
    assert add_state.validate_state("fl")
    assert not add_state.validate_state("FL")
    assert not add_state.validate_state("f")
    assert not add_state.validate_state("florida")
