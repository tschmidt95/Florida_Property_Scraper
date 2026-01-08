from .alachua import parse as alachua_parse
from .broward import parse as broward_parse
from .seminole import parse as seminole_parse
from .orange import parse as orange_parse
from .palm_beach import parse as palm_beach_parse
from .miami_dade import parse as miami_dade_parse
from .hillsborough import parse as hillsborough_parse
from .pinellas import parse as pinellas_parse


PARSERS = {
    "alachua": alachua_parse,
    "broward": broward_parse,
    "seminole": seminole_parse,
    "orange": orange_parse,
    "palm_beach": palm_beach_parse,
    "miami_dade": miami_dade_parse,
    "hillsborough": hillsborough_parse,
    "pinellas": pinellas_parse,
}


def get_parser(county_slug):
    parser = PARSERS.get(county_slug)
    if parser is None:
        raise KeyError(f"No native parser for {county_slug}")
    return parser
