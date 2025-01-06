from geopy.geocoders import Nominatim

def get_country_center_coordinates(country_name: str = 'Netherlands'):
    """
    Takes country name and returns its central latitude and longitude (rounded to 2 digits after dot).
    """
    geolocator = Nominatim(user_agent="MyApp")
    location = geolocator.geocode(country_name)

    if location:
        latitude = round(location.latitude, 2)
        longitude = round(location.longitude, 2)
        return latitude, longitude
    else:
        raise ValueError(f"Could not find coordinates for {country_name}")