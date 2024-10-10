from repository.accidents_repository import get_sum_accidents_by_area, get_sum_accidents_by_area_and_day, \
    get_sum_accidents_by_area_and_start_date_for_week, get_sum_accidents_by_area_and_month, \
    get_accidents_and_reason_by_area, get_accidents_and_stats_injury_by_area


def test_get_sum_accidents_by_area():
    result = get_sum_accidents_by_area("1235")
    assert len(result) == 1
    assert result[0]['accidents'] == 10

def test_get_sum_accidents_by_area_and_day():
    result = get_sum_accidents_by_area_and_day("1235", "2023-09-05")
    assert len(result) == 1
    assert result[0]['accidents'] == 3

def test_get_sum_accidents_by_area_and_start_date_for_week():
    result = get_sum_accidents_by_area_and_start_date_for_week("1235", "2023-09-01")
    assert len(result) == 1
    assert result[0]['accidents'] == 5

def test_get_sum_accidents_by_area_and_month():
    result = get_sum_accidents_by_area_and_month("1235", "2023-09")
    assert len(result) == 1
    assert result[0]['accidents'] == 7

def test_get_accidents_and_reason_by_area():
    result = get_accidents_and_reason_by_area("1235")
    assert result['total_accidents'] == 4
    assert len(result['accidents']) == 4
    assert result['accidents'][0]['reason'] == 'WEATHER'

def test_get_accidents_and_stats_injury_by_area():
    result = get_accidents_and_stats_injury_by_area("1652")
    assert result['total_accidents'] == 55
    assert len(result['accidents']) == 55
    assert result['accidents'][0]['injuries'] == 0