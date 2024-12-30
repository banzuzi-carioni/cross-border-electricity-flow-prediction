from geopy.geocoders import Nominatim


BZN2CITY = {
    'FI': 'Finland',
    'SE_1': 'Luleå',
    'SE_2': 'Sundsvall',
    'SE_3': 'Stockholm',
    'SE_4': 'Malmö',
    'DK_1': 'Aalborg',
    'DK_2': 'Copenhagen',
    'NO_1': 'Oslo',
    'NO_2': 'Kristiansand',
    'NO_3': 'Molde',
    'NO_4': 'Tromsø',
    'NO_5': 'Bergen'
}


def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    # Initialize Nominatim API (for getting lat and long of the city)
    geolocator = Nominatim(user_agent="MyApp")
    city_name = BZN2CITY.get(city_name, city_name)
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)

    return longitude, latitude
