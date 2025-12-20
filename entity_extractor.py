import spacy
from geopy.geocoders import Nominatim
import dateparser
from datetime import datetime
import re
from typing import List, Dict, Tuple, Optional


class EntityExtractor:
    def __init__(self):
        # Load spaCy model
        self.nlp = spacy.load("en_core_web_sm")

        # Set up geocoder for converting location names to coordinates
        self.geolocator = Nominatim(user_agent="smart_document_retrieval")

    def extract_temporal_expressions(self, text: str) -> Tuple[List[str], List[datetime]]:
        """
        Extract temporal expressions from text
        Returns: (list of temporal expressions, list of parsed dates)
        """
        doc = self.nlp(text)
        temporal_expressions = []
        parsed_dates = []

        # Extract dates using spaCy DATE entities
        for ent in doc.ents:
            if ent.label_ == "DATE":
                temporal_expressions.append(ent.text)
                parsed_date = dateparser.parse(ent.text)
                if parsed_date:
                    parsed_dates.append(parsed_date)

        # Find extra date patterns using regex
        date_patterns = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # DD/MM/YYYY or MM/DD/YYYY
            r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',    # YYYY-MM-DD
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b',
            r'\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}\b'
        ]

        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match not in temporal_expressions:
                    temporal_expressions.append(match)
                    parsed_date = dateparser.parse(match)
                    if parsed_date:
                        parsed_dates.append(parsed_date)

        return temporal_expressions, parsed_dates

    def extract_georeferences(self, text: str) -> Tuple[List[str], List[Dict]]:
        """
        Extract geographic references from text
        Returns: (list of location names, list of coordinates)
        """
        doc = self.nlp(text)
        georeferences = []
        coordinates = []

        # Extract location entities (countries, cities, places, facilities)
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC", "FAC"]:
                location_name = ent.text
                if location_name not in georeferences:
                    georeferences.append(location_name)

                    # Try to geocode the location name into lat/lon
                    try:
                        location = self.geolocator.geocode(location_name, timeout=5)
                        if location:
                            coordinates.append({
                                "name": location_name,
                                "lat": location.latitude,
                                "lon": location.longitude
                            })
                    except Exception as e:
                        print(f"Error geocoding {location_name}: {e}")

        return georeferences, coordinates

    def extract_all_entities(self, text: str) -> Dict:
        """
        Extract all entities from text (dates + locations)
        """
        temporal_expressions, parsed_dates = self.extract_temporal_expressions(text)
        georeferences, coordinates = self.extract_georeferences(text)

        return {
            "temporal_expressions": temporal_expressions,
            "parsed_dates": parsed_dates,
            "georeferences": georeferences,
            "coordinates": coordinates
        }

    def approximate_date(self, doc_date: Optional[datetime], parsed_dates: List[datetime]) -> Optional[datetime]:
        """
        Choose a best date when the document date is missing
        """
        if doc_date:
            return doc_date
        if parsed_dates:
            return max(parsed_dates)
        return None

    def approximate_location(self, doc_location: Optional[Dict], coordinates: List[Dict]) -> Optional[Dict]:
        """
        Choose a best location when the document location is missing
        """
        if doc_location:
            return doc_location
        if coordinates:
            return {"lat": coordinates[0]["lat"], "lon": coordinates[0]["lon"]}
        return None



