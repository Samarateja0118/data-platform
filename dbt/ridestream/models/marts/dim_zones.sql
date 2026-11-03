select zone_id, zone_name, district, is_airport
from {{ ref('zones') }}
