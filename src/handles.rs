pub fn normalize_handle(input: &str) -> Option<String> {
    let mut value = input
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    if value.is_empty() {
        return None;
    }

    if let Some(at_index) = value.find("/@") {
        value = value[(at_index + 2)..].to_string();
    } else if let Some(at_index) = value.find('@') {
        value = value[(at_index + 1)..].to_string();
    }

    if let Some(separator) = value.find(['?', '#', '/', '&']) {
        value.truncate(separator);
    }

    let normalized: String = value
        .trim_start_matches('@')
        .chars()
        .filter_map(|ch| {
            let lower = ch.to_ascii_lowercase();
            if lower.is_ascii_alphanumeric() || lower == '.' || lower == '_' {
                Some(lower)
            } else {
                None
            }
        })
        .collect();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

pub fn normalize_many<'a>(handles: impl IntoIterator<Item = &'a String>) -> Vec<String> {
    let mut normalized = handles
        .into_iter()
        .filter_map(|handle| normalize_handle(handle))
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_common_tiktok_handle_inputs() {
        assert_eq!(
            normalize_handle("@Creator.Name_1"),
            Some("creator.name_1".into())
        );
        assert_eq!(
            normalize_handle("https://www.tiktok.com/@SomeCreator/video/123"),
            Some("somecreator".into())
        );
        assert_eq!(
            normalize_handle("https://www.tiktok.com/@SomeCreator?lang=en"),
            Some("somecreator".into())
        );
        assert_eq!(normalize_handle(" "), None);
    }
}
