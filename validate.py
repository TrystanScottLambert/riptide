"""
Module for validating parquet data ensuring tables follow the style standards.
"""

from dataclasses import dataclass


from filter_check import validate_filter, valid_filters
MAX_COLUMN_LENGTH = 30
EXCEPTIONS = ["uberID"]
NOT_ALLOWED = ["fred", "bob", "thing", "something"]
PROTECTED_WORD_LIST = {
    "ra": [
        "ascension", "r.a.", "r a", "ra_deg", "ra (deg)", "ra degrees", "right_ascension", "ra_degrees"
    ],
    "dec": [
        "declination", "dec_deg", "dec (deg)", "dec degrees", "decl."
    ],
    "vel": [
        "velocity", "vel_kms", "v_los", "vlos", "v_rad", "v(km/s)", "radial velocity"
    ],
    "mag": [
        "magnitude", "app_mag", "apparent magnitude", "m_app"
    ],
    "mag_abs": [
        "absolute magnitude", "m_abs", "abs_mag", "Mmag", "M_abs"
    ],
    "err": [
        "error", "uncertainty", "sigma", "stddev", "std", "measurement error"
    ],
    "flux_density": [
        "flux density", "f_nu", "fnu", "f_lambda", "flam", "fluxdens", "Snu"
    ],
    "flux": [
        "total flux", "measured flux", "int_flux", "integrated flux", "f"
    ],
    "luminosity": [
        "lum", "luminosity (erg/s)", "Lbol", "L_sun", "bolometric luminosity"
    ],
    "mass": [
        "stellar mass", "M", "Msol", "mass_msun", "mstar", "Mstar"
    ],
    "sfr": [
        "star formation rate", "SFR", "SFR_msun_yr", "sfr(msun/yr)"
    ],
    "metallicity": [
        "gas metallicity", "stellar metallicity", "metal abundance"
    ],
    "redshift": [
        "zobs", "z_spec", "z_phot", "spectroscopic redshift", "photometric redshift", "zcos", "z_obs"
    ],
    "snr": [
        "signal to noise", "S/N", "sn_ratio", "signal/noise", "sn"
    ],
    "ew": [
        "equivalent width", "eq width", "EW_line", "eqw"
    ],
    "radius": [
        "rad", "r_phys", "r_ang", "radii", "object radius", "r (arcsec)", "r (kpc)"
    ],
    "sersic_index": [
        "n_sersic", "sersic n", "sersicn", "Sérsic index", "SersicN"
    ],
    "axrat": [
        "axis ratio", "axial ratio", "b/a", "ellipticity", "axisratio"
    ],
    "ang": [
        "angle", "angular measurement", "ang (deg)", "angular size", "theta", "phi"
    ],
    "pos_ang": [
        "position angle", "pa", "posang", "P.A.", "PA(deg)", "position angle (deg)"
    ],
    "line_width": [
        "linewidth", "line width", "FWHM", "sigma_line", "velocity width", "dispersion"
    ],
    "sep": [
        "separation", "sep_dist", "distance between", "angular separation", "physical separation"
    ]
}


def check_allowed(name: str) -> bool:
    """
    Checks that the list of not allowed words isn't being used.
    """
    for na in NOT_ALLOWED:
        if na in name:
            return False, na
    return True, None

def check_protected(name: str) -> bool:
    """
    Checks that protected names aren't being used in the tables.
    """
    for protected_word, common_words in PROTECTED_WORD_LIST.items():
        for word in common_words:
            for target_word in name.split('_'):
                if word.lower() == target_word.lower():
                    return (False, protected_word)
    return (True, None)


def check_exceptions(name: str) -> bool:
    """
    Checks that if the exceptions exist that they are in the correct case.
    """
    real_string = name.replace("_", "")
    for exc in EXCEPTIONS:
        if exc.lower() in real_string.lower():
            if exc in real_string:
                return True, None
            return False, exc
    return True, None


def check_alphanumeric(name: str) -> bool:
    """
    Checks that the given string is alpha numeric (excepting underscore)
    """
    no_underscores = name.replace("_", "")
    return no_underscores.isalnum()


def check_alphabetical_start(name: str) -> bool:
    """
    Checks that the string doesn't start with a number.
    """
    return name[0].isalpha()


def check_snake_case(name: str) -> bool:
    """
    Checks that the name is in snake case excluding the filter names and the exceptions list
    """
    if name.startswith("_"):
        return False
    if name.endswith("_"):
        return False
    if "__" in name:
        return False
    actual_string = name
    for filter_name in valid_filters:
        actual_string = actual_string.replace(filter_name, "")
    for exception in EXCEPTIONS:
        actual_string = actual_string.replace(exception, "")
    if actual_string == "":
        return True
    if actual_string.islower():
        if not check_alphanumeric(actual_string):
            return False
        return True
    return False


@dataclass
class ColumnNameReport:
    """
    Checks for given column name
    """

    name: str
    valid: bool
    alpha_numeric: bool
    starts_with_letter: bool
    snake_case: bool  # taking into account filters and exceptions
    length: bool
    no_decimals: bool
    filter_name: bool
    allowed_words: bool
    no_exception_violation: bool
    not_protected: bool
    not_allowed_words: str | None
    suggested_filter_name: str | None
    exception_word: str | None
    protected_word: str | None 

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
        def status(passed: bool) -> str:
            if passed:
                return f"{GREEN}✓ PASS{RESET}"
            return f"{RED}✗ FAIL{RESET}"

        # Print header
        print(f"\n{BOLD}{'='*70}{RESET}")
        print(f"{BOLD}Column Name Validation Report{RESET}")
        print(f"{BOLD}{'='*70}{RESET}")

        # Overall status
        overall_color = GREEN if self.valid else RED
        overall_status = "VALID" if self.valid else "INVALID"
        print(f"\n{BOLD}Column Name:{RESET} {self.name}")
        print(f"{BOLD}Overall Status:{RESET} {overall_color}{overall_status}{RESET}")

        # Validation checks
        print(f"\n{BOLD}Validation Checks:{RESET}")
        print(f"{'-'*70}")

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
        length_info = f" (length: {len(self.name)}/30)" if not self.length else ""
        print(
            f"  Length < 30 characters:                       {length_status}{length_info}"
        )

        print(
            f"  No decimal points:                            {status(self.no_decimals)}"
        )

        filter_status = status(self.filter_name)
        filter_info = ""
        if not self.filter_name and self.suggested_filter_name:
            filter_info = (
                f"\n    {YELLOW}→ Suggestion: Use '{self.suggested_filter_name}'{RESET}"
            )
        print(
            f"  Valid filter name usage:                      {filter_status}{filter_info}"
        )

        exception_status = status(self.no_exception_violation)
        exception_info = ""
        if not self.no_exception_violation and self.exception_word:
            exception_info = f"\n    {YELLOW}→ Required: Use correct case '{self.exception_word}'{RESET}"
        print(
            f"  Exception words in correct case:              {exception_status}{exception_info}"
        )

        
        protected_status = status(self.not_protected)
        protected_info = ""
        if not self.not_protected and self.protected_word:
            protected_info = f"\n    {YELLOW}→ Required: Use correct form maybe '{self.protected_word}'?{RESET}"
        print(
            f"  Not violating protected standards:            {protected_status}{protected_info}"
        )


        allowed_status = status(self.allowed_words)
        allowed_info = ""
        if not self.allowed_words and self.not_allowed_words:
            allowed_info = f"\n    {YELLOW}→ Contains banned word: '{self.not_allowed_words}'{RESET}"
        print(
            f"  No banned words:                              {allowed_status}{allowed_info}"
        )

        print(f"{BOLD}{'='*70}{RESET}\n")


def check_column_name(name: str) -> ColumnNameReport:
    """
    Checks that the column names are correct and returns a report
    """
    alphanumeric = check_alphanumeric(name)
    letter_start = check_alphabetical_start(name)
    valid_length = len(name) < MAX_COLUMN_LENGTH
    snake_case = check_snake_case(name)
    no_decimals = "." not in name
    valid_filter, suggested_filter = validate_filter(name)
    allowed, banned_word = check_allowed(name)
    violates_exception, exception_word = check_exceptions(name)
    not_protected_word, protected_word = check_protected(name)
    is_valid = all(
        [
            alphanumeric,
            letter_start,
            valid_length,
            snake_case,
            no_decimals,
            valid_filter,
            allowed,
            violates_exception,
            not_protected_word,
        ]
    )
    return ColumnNameReport(
        name=name,
        valid=is_valid,
        alpha_numeric=alphanumeric,
        starts_with_letter=letter_start,
        snake_case=snake_case,
        length=valid_length,
        no_decimals=no_decimals,
        filter_name=valid_filter,
        suggested_filter_name=suggested_filter,
        allowed_words=allowed,
        not_allowed_words=banned_word,
        no_exception_violation=violates_exception,
        exception_word=exception_word,
        not_protected=not_protected_word,
        protected_word=protected_word,
    )


