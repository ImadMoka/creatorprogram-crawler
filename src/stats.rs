use crate::models::{CreatorStats, TiktokPost};

pub fn calculate_creator_stats(posts: &[TiktokPost]) -> CreatorStats {
    if posts.is_empty() {
        return CreatorStats {
            avg_views: 0.0,
            median_views: 0.0,
            most_viral_video_url: None,
            most_viral_video_views: None,
        };
    }

    let mut views = posts.iter().map(|post| post.views).collect::<Vec<_>>();
    views.sort_unstable();

    let total = views.iter().sum::<u64>() as f64;
    let avg_views = total / views.len() as f64;
    let median_views = median(&views);
    let most_viral = posts.iter().max_by_key(|post| post.views);

    CreatorStats {
        avg_views,
        median_views,
        most_viral_video_url: most_viral.map(|post| post.url.clone()),
        most_viral_video_views: most_viral.map(|post| post.views),
    }
}

fn median(sorted_values: &[u64]) -> f64 {
    let len = sorted_values.len();
    let mid = len / 2;
    if len.is_multiple_of(2) {
        (sorted_values[mid - 1] as f64 + sorted_values[mid] as f64) / 2.0
    } else {
        sorted_values[mid] as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TiktokPostKind;

    fn post(views: u64) -> TiktokPost {
        TiktokPost {
            id: None,
            url: format!("https://tiktok.test/video/{views}"),
            caption: None,
            views,
            published_at: None,
            kind: TiktokPostKind::Video,
            is_pinned: false,
            source_url: None,
            slide_image_urls: Vec::new(),
            visual_image_urls: Vec::new(),
            raw: serde_json::Value::Null,
        }
    }

    #[test]
    fn calculates_average_median_and_most_viral() {
        let stats = calculate_creator_stats(&[post(10), post(30), post(20), post(100)]);

        assert_eq!(stats.avg_views, 40.0);
        assert_eq!(stats.median_views, 25.0);
        assert_eq!(stats.most_viral_video_views, Some(100));
    }
}
