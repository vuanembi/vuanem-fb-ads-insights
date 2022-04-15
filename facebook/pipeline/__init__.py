from facebook.pipeline import (
    ads_insights,
    age_gender_insights,
    device_insights,
    platform_position_insights,
    region_insights,
    # video_insights,
)

pipelines = {
    i.name: i
    for i in [
        ads_insights.ads_insights,
        age_gender_insights.age_gender_insights,
        device_insights.device_insights,
        platform_position_insights.platform_position_insights,
        region_insights.region_insights,
        # video_insights.video_insights,
    ]
}
