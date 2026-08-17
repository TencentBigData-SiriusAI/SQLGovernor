"""String normalization and fuzzy matching helpers for result comparison.

Provides location/school-type normalization, synonym expansion, and
fuzzy string matching used by the value-comparison logic.
"""

import re
from typing import List, Dict, Tuple, Optional
from difflib import SequenceMatcher


class StringMatcher:
    """Normalize and fuzzily match string values."""

    # Common location variants normalized to a canonical form.
    LOCATION_NORMALIZATIONS = {
        "alameda county": ["alameda", "alameda county", "alameda co", "alameda co."],
        "fresno county": ["fresno", "fresno county", "fresno co", "fresno co."],
        "los angeles": ["los angeles", "los angeles county", "la county", "la"],
        "san francisco": ["san francisco", "san francisco county", "sf county", "sf"],
    }

    # School-type variants normalized to a canonical form.
    SCHOOL_TYPE_NORMALIZATIONS = {
        "continuation school": ["continuation", "continuation school", "cont school"],
        "charter school": ["charter", "charter school"],
        "magnet school": ["magnet", "magnet school"],
    }

    # Synonym groups expanded during matching.
    SYNONYMS = {
        "directly funded": ["direct funded", "directly funded", "direct charter funded"],
        "office of education": ["office of education", "county office of education", "educational office"],
    }

    @classmethod
    def normalize_location(cls, location: str) -> str:
        """Normalize a location string to its canonical form.

        Args:
            location: The location string.

        Returns:
            The canonical form if matched, else the lowercased input.
        """
        location_lower = location.lower().strip()

        for standard_form, variants in cls.LOCATION_NORMALIZATIONS.items():
            if any(variant in location_lower for variant in variants):
                return standard_form

        return location_lower

    @classmethod
    def normalize_school_type(cls, school_type: str) -> str:
        """Normalize a school-type string to its canonical form.

        Args:
            school_type: The school-type string.

        Returns:
            The canonical form if matched, else the lowercased input.
        """
        school_type_lower = school_type.lower().strip()

        for standard_form, variants in cls.SCHOOL_TYPE_NORMALIZATIONS.items():
            if any(variant in school_type_lower for variant in variants):
                return standard_form

        return school_type_lower

    @classmethod
    def fuzzy_match(cls, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Return whether two strings are approximately equal.

        Args:
            text1: The first string.
            text2: The second string.
            threshold: Similarity ratio above which the strings match.

        Returns:
            True if the similarity ratio is >= threshold.
        """
        # Strip punctuation and normalize case.
        text1_clean = re.sub(r'[^\w\s]', '', text1.lower().strip())
        text2_clean = re.sub(r'[^\w\s]', '', text2.lower().strip())

        # Exact match after cleanup.
        if text1_clean == text2_clean:
            return True

        # Fuzzy match via sequence similarity.
        similarity = SequenceMatcher(None, text1_clean, text2_clean).ratio()
        return similarity >= threshold

    @classmethod
    def find_best_match(cls, target: str, candidates: List[str], threshold: float = 0.7) -> Optional[str]:
        """Find the best fuzzy match for a target among candidates.

        Args:
            target: The target string.
            candidates: Candidate strings to match against.
            threshold: Minimum similarity ratio to accept.

        Returns:
            The best matching candidate, or None.
        """
        target_clean = re.sub(r'[^\w\s]', '', target.lower().strip())
        best_match = None
        best_score = 0

        for candidate in candidates:
            candidate_clean = re.sub(r'[^\w\s]', '', candidate.lower().strip())

            similarity = SequenceMatcher(None, target_clean, candidate_clean).ratio()

            if similarity > best_score and similarity >= threshold:
                best_score = similarity
                best_match = candidate

        return best_match

    @classmethod
    def generate_variations(cls, text: str) -> List[str]:
        """Generate plausible string variations for fuzzy matching.

        Args:
            text: The input string.

        Returns:
            A list of variations including the original.
        """
        variations = [text]
        text_lower = text.lower().strip()

        # Variation without punctuation.
        no_punct = re.sub(r'[^\w\s]', '', text_lower)
        if no_punct != text_lower:
            variations.append(no_punct)

        # Drop "County"
        if "county" in text_lower:
            without_county = text_lower.replace("county", "").strip()
            variations.append(without_county)

        # Drop "School"
        if "school" in text_lower:
            without_school = text_lower.replace("school", "").strip()
            variations.append(without_school)

        # Drop "Office of Education"
        if "office of education" in text_lower:
            without_office = text_lower.replace("office of education", "").strip()
            variations.append(without_office)

        return list(set(variations))  # deduplicate

    @classmethod
    def get_smart_where_conditions(cls, field_name: str, target_value: str) -> List[str]:
        """Generate a list of WHERE conditions for a field and target value.

        Args:
            field_name: The field name.
            target_value: The target value.

        Returns:
            A list of SQL WHERE conditions.
        """
        conditions = []

        # Exact equality first.
        conditions.append(f"{field_name} = '{target_value}'")

        # LIKE conditions over variations.
        variations = cls.generate_variations(target_value)

        for variation in variations:
            if variation != target_value:
                # LIKE match
                conditions.append(f"{field_name} LIKE '%{variation}%'")

                # Exact equality for short variations.
                if len(variation.split()) <= 2:
                    conditions.append(f"{field_name} = '{variation}'")

        return conditions

    @classmethod
    def suggest_field_corrections(cls, failed_field: str, schema_fields: List[str]) -> List[Tuple[str, float]]:
        """Suggest schema fields similar to a failed field name.

        Args:
            failed_field: The field name that failed to match.
            schema_fields: Valid schema field names.

        Returns:
            A list of (field, similarity) tuples.
        """
        suggestions = []
        failed_clean = re.sub(r'[^\w\s]', '', failed_field.lower().strip())

        for field in schema_fields:
            field_clean = re.sub(r'[^\w\s]', '', field.lower().strip())
            similarity = SequenceMatcher(None, failed_clean, field_clean).ratio()

            if similarity > 0.5:  # minimum similarity threshold
                suggestions.append((field, similarity))

        # Sort by descending similarity.
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:3]  # top 3

    @classmethod
    def create_flexible_sql_conditions(cls, field_conditions: Dict[str, str]) -> List[str]:
        """Build SQL WHERE conditions from field-to-value mappings.

        Args:
            field_conditions: Mapping of {field_name: target_value}.

        Returns:
            A list of SQL WHERE conditions.
        """
        all_conditions = []

        for field_name, target_value in field_conditions.items():
            field_conditions_list = cls.get_smart_where_conditions(field_name, target_value)

            # Group alternatives with OR.
            if len(field_conditions_list) > 1:
                or_condition = "(" + " OR ".join(field_conditions_list) + ")"
                all_conditions.append(or_condition)
            else:
                all_conditions.extend(field_conditions_list)

        return all_conditions


# Singleton instance for convenience.
string_matcher = StringMatcher()
