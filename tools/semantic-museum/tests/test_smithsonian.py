from __future__ import annotations

from semantic_museum.smithsonian import normalize_smithsonian_record


def test_normalizer_requires_record_and_media_cc0() -> None:
    record = {
        "content": {
            "descriptiveNonRepeating": {
                "record_ID": "item",
                "metadata_usage": {"access": "CC0"},
                "online_media": {
                    "media": [
                        {
                            "type": "Images",
                            "idsId": "image",
                            "content": "https://ids.si.edu/ids/deliveryService?id=x&max=9999",
                            "usage": {"access": "CC0"},
                        }
                    ]
                },
            }
        }
    }
    normalized = normalize_smithsonian_record(
        record, thumbnail_size=256, media_policy="primary"
    )
    assert len(normalized) == 1
    assert "max=256" in normalized[0].image_url

    descriptive = record["content"]["descriptiveNonRepeating"]
    descriptive["metadata_usage"]["access"] = "Usage conditions apply"
    assert (
        normalize_smithsonian_record(
            record, thumbnail_size=256, media_policy="primary"
        )
        == []
    )
