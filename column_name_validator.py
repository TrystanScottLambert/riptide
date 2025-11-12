"""
Module for validating parquet data ensuring tables follow the style standards.
"""

from dataclasses import dataclass

from thefuzz import fuzz

from status import Status, State
from filter_check import check_filter, valid_filters

MAX_COLUMN_LENGTH = 50
WARN_COLUMN_LENGTH = 25
EXCEPTIONS = ["uberID"]
NOT_ALLOWED = [
    "fred",
    "bob",
    "thing",
    "something",
    "whatever",
    "words",
    "blahblahblah",
    "abc123",
    "xyz",
]
PROTECTED_WORD_LIST = {
    "ra": [
        "ascension",
        "r.a.",
        "r a",
        "ra_deg",
        "ra (deg)",
        "ra degrees",
        "right_ascension",
        "ra_degrees",
    ],
    "dec": ["declination", "dec_deg", "dec (deg)", "dec degrees", "decl."],
    "vel": [
        "velocity",
        "vel_kms",
        "v_los",
        "vlos",
        "v_rad",
        "v(km/s)",
        "radial velocity",
    ],
    "mag": ["magnitude", "app_mag", "apparent magnitude", "m_app"],
    "mag_abs": ["absolute magnitude", "m_abs", "abs_mag", "Mmag", "M_abs"],
    "err": ["error", "uncertainty", "stddev", "std", "measurement error"],
    "flux_density": [
        "flux density",
        "f_nu",
        "fnu",
        "f_lambda",
        "flam",
        "fluxdens",
        "Snu",
    ],
    "flux": ["total flux", "measured flux", "int_flux", "integrated flux", "f"],
    "luminosity": [
        "lum",
        "luminosity (erg/s)",
        "Lbol",
        "L_sun",
        "bolometric luminosity",
    ],
    "mass": ["stellar mass", "M", "Msol", "mass_msun", "mstar", "Mstar", "msun"],
    "sfr": ["star formation rate", "SFR_msun_yr", "sfr(msun/yr)"],
    "metallicity": ["gas metallicity", "stellar metallicity", "metal abundance"],
    "redshift": [
        "zobs",
        "z_spec",
        "z_phot",
        "spectroscopic redshift",
        "photometric redshift",
        "zcos",
        "z_obs",
    ],
    "snr": ["signal to noise", "S/N", "sn_ratio", "signal/noise", "sn"],
    "ew": ["equivalent width", "eq width", "EW_line", "eqw"],
    "radius": [
        "rad",
        "r_phys",
        "r_ang",
        "radii",
        "object radius",
        "r (arcsec)",
        "r (kpc)",
    ],
    "sersic_index": ["n_sersic", "sersic n", "sersicn", "Sérsic index", "SersicN"],
    "axrat": ["axis ratio", "axial ratio", "b/a", "ellipticity", "axisratio"],
    "ang": [
        "angle",
        "angular measurement",
        "ang (deg)",
        "angular size",
        "theta",
        "phi",
    ],
    "pos_ang": [
        "position angle",
        "pa",
        "posang",
        "P.A.",
        "PA(deg)",
        "position angle (deg)",
    ],
    "line_width": [
        "linewidth",
        "line width",
        "FWHM",
        "sigma_line",
        "velocity width",
        "dispersion",
    ],
    "sep": [
        "separation",
        "sep_dist",
        "distance between",
        "angular separation",
        "physical separation",
    ],
}


def check_length(name: str) -> Status:
    """
    Checks that the lengths is less than the max length. Warns if more than 25.
    """
    name_length = len(name)
    if name_length < WARN_COLUMN_LENGTH:
        return Status(State.PASS)
    if name_length > MAX_COLUMN_LENGTH:
        return Status(State.FAIL)
    return Status(State.WARNING)


def check_decimals(name: str) -> Status:
    if "." not in name:
        return Status(State.PASS)
    return Status(State.FAIL)


def check_allowed(name: str) -> Status:
    """
    Checks that the list of not allowed words isn't being used.
    """
    for na in NOT_ALLOWED:
        if na in name:
            return Status(State.FAIL, na)
    # fuzzy searching:
    for na in NOT_ALLOWED:
        for word in name.split("_"):
            ratio = fuzz.ratio(na, word.lower())
            if ratio > 80:
                return Status(State.FAIL, na)
            if ratio > 50:
                return Status(State.WARNING, na)

    return Status(State.PASS)


def check_protected(name: str) -> Status:
    """
    Checks that protected names aren't being used in the tables.
    """
    for protected_word, common_words in PROTECTED_WORD_LIST.items():
        for word in common_words:
            if word == name:
                return Status(State.FAIL, protected_word)
            for target_word in name.split("_"):
                if word.lower() == target_word.lower():
                    return Status(State.WARNING, protected_word)
    return Status(State.PASS)


def check_exceptions(name: str) -> Status:
    """
    Checks that if the exceptions exist that they are in the correct case.
    """
    real_string = name.replace("_", "")
    for exc in EXCEPTIONS:
        if exc.lower() in real_string.lower():
            if exc in real_string:
                return Status(State.PASS)
            return Status(State.FAIL, exc)
    return Status(State.PASS)


def check_alphanumeric(name: str) -> Status:
    """
    Checks that the given string is alpha numeric (excepting underscore)
    """
    no_underscores = name.replace("_", "")
    if no_underscores.isalnum():
        return Status(State.PASS)
    return Status(State.FAIL)


def check_alphabetical_start(name: str) -> Status:
    """
    Checks that the string doesn't start with a number.
    """
    if name[0].isalpha():
        return Status(State.PASS)
    return Status(State.FAIL)


def check_snake_case(name: str) -> Status:
    """
    Checks that the name is in snake case excluding the filter names and the exceptions list
    """
    if name.startswith("_"):
        return Status(State.FAIL, "Starts with underscore.")
    if name.endswith("_"):
        return Status(State.FAIL, "Ends with underscore.")
    if "__" in name:
        return Status(State.FAIL, "Multiple underscores in a row.")
    actual_string = name
    for filter_name in valid_filters:
        actual_string = actual_string.replace(filter_name, "")
    for exception in EXCEPTIONS:
        actual_string = actual_string.replace(exception, "")
    if actual_string == "":
        return Status(State.PASS)
    if actual_string.islower():
        if not check_alphanumeric(actual_string):
            return Status(State.FAIL)
        return Status(State.PASS)
    return Status(State.FAIL)


@dataclass
class ColumnNameReport:
    """
    Checks for given column name
    """

    name: str
    alpha_numeric: Status
    starts_with_letter: Status
    snake_case: Status  # taking into account filters and exceptions
    length: Status
    no_decimals: Status
    filter_name: Status
    allowed_words: Status
    no_exception_violation: Status
    not_protected: Status

    def __post_init__(self) -> None:
        self.valid: bool = all(
            [
                self.alpha_numeric == State.PASS,
                self.starts_with_letter == State.PASS,
                self.snake_case == State.PASS,
                self.length == State.PASS,
                self.no_decimals == State.PASS,
                self.filter_name == State.PASS,
                self.allowed_words == State.PASS,
                self.no_exception_violation == State.PASS,
                self.not_protected == State.PASS,
            ]
        )

    def print_report(self):
        """
        Print a professional validation report with color-coded results.
        """
        # ANSI color codes
        GREEN = "\033[92m"
        RED = "\033[91m"
        BOLD = "\033[1m"
        RESET = "\033[0m"
        YELLOW = "\033[93m"

        # Helper function for status
        def status(given_status: Status) -> str:
            match given_status.state:
                case State.PASS:
                    return f"{GREEN}✓ PASS{RESET}"
                case State.FAIL:
                    return f"{RED}✗ FAIL{RESET}"
                case State.WARNING:
                    return f"{YELLOW}⚠ WARNING"

        # Overall status
        overall_color = GREEN if self.valid else RED
        overall_status = "VALID" if self.valid else "INVALID"
        print(f"\n{BOLD}Column Name:{RESET} {self.name}")
        print(f"{BOLD}Overall Status:{RESET} {overall_color}{overall_status}{RESET}")

        # Validation checks
        print(f"\n{BOLD}Validation Checks:{RESET}")
        print(f"{'-' * 70}")

        print(
            f"  Alphanumeric (letters, numbers, underscores): {status(self.alpha_numeric)}"
        )

        print(
            f"  Starts with letter:                           {status(self.starts_with_letter)}"
        )

        print(
            f"  Snake case format:                            {status(self.snake_case)}"
        )

        length_status = status(self.length)
        length_info = ""
        if self.length.state == State.WARNING:
            length_info = f"\n    {YELLOW} → Length is valid but long ({len(self.name)}/{MAX_COLUMN_LENGTH}).{RESET}"
        if self.length.state == State.FAIL:
            length_info = f"\n     {RED} → Column name is too long ({len(self.name)}/{MAX_COLUMN_LENGTH}).{RESET}"
        print(
            f"  Length < {MAX_COLUMN_LENGTH} characters:                       {length_status}{length_info}"
        )

        print(
            f"  No decimal points:                            {status(self.no_decimals)}"
        )

        filter_status = status(self.filter_name)
        filter_info = ""
        if self.filter_name.state == State.FAIL:
            filter_info = (
                f"\n    {RED} → Required: Use '{self.filter_name.message}'{RESET}"
            )
        if self.filter_name.state == State.WARNING:
            filter_info = f"\n    {YELLOW} → Possible filter name violation: did you mean '{self.filter_name.message}'?{RESET}"

        print(
            f"  Valid filter name usage:                      {filter_status}{filter_info}"
        )

        exception_status = status(self.no_exception_violation)
        exception_info = ""
        if self.no_exception_violation.state != State.PASS:
            exception_info = f"\n    {RED} → Required: Use correct case '{self.no_exception_violation.message}'{RESET}"
        print(
            f"  Exception words in correct case:              {exception_status}{exception_info}"
        )

        protected_status = status(self.not_protected)
        protected_info = ""
        if self.not_protected.state == State.WARNING:
            protected_info = f"\n    {YELLOW} → Protected word in use: Use correct form. Maybe '{self.not_protected.message}'?{RESET}"
        if self.not_protected.state == State.FAIL:
            protected_info = f"\n    {RED} → Protected word in use: Use correct case: '{self.not_protected.message}'{RESET}"
        print(
            f"  Not violating protected standards:            {protected_status}{protected_info}"
        )

        allowed_status = status(self.allowed_words)
        allowed_info = ""
        if self.allowed_words.state == State.FAIL:
            allowed_info = f"\n    {RED} → Contains banned word: '{self.allowed_words.message}'{RESET}"
        if self.allowed_words.state == State.WARNING:
            allowed_info = f"\n    {YELLOW} → Possible banned word: '{self.allowed_words.message}'{RESET}"
        print(
            f"  No banned words:                              {allowed_status}{allowed_info}"
        )

        print(f"{'-' * 70}")


def validate_column_name(name: str) -> ColumnNameReport:
    """
    Checks that the column names are correct and returns a report
    """
    alphanumeric = check_alphanumeric(name)
    letter_start = check_alphabetical_start(name)
    valid_length = check_length(name)
    snake_case = check_snake_case(name)
    no_decimals = check_decimals(name)
    valid_filter = check_filter(name)
    allowed = check_allowed(name)
    violates_exception = check_exceptions(name)
    not_protected_word = check_protected(name)

    return ColumnNameReport(
        name=name,
        alpha_numeric=alphanumeric,
        starts_with_letter=letter_start,
        snake_case=snake_case,
        length=valid_length,
        no_decimals=no_decimals,
        filter_name=valid_filter,
        allowed_words=allowed,
        no_exception_violation=violates_exception,
        not_protected=not_protected_word,
    )
