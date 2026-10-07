//! The cross-crate path the auto_connect demo relies on: adverts from
//! SEPARATE crates survive linking and are collected — given the
//! load-bearing `use ... as _;` anchors.

use spiderweb_registry::advertised;

// The same anchors the demo uses. Without these, the linker can
// dead-strip the adverts from crates this test never otherwise names.
use cap_sink as _;
use cap_source as _;

#[test]
fn cross_crate_adverts_survive_linking() {
    let names: std::collections::BTreeSet<&str> = advertised().map(|a| a.name).collect();
    assert!(
        names.contains("cap-source"),
        "cap-source's self-advert should be collected across the crate boundary, got {names:?}"
    );
    assert!(
        names.contains("cap-sink"),
        "cap-sink's self-advert should be collected across the crate boundary, got {names:?}"
    );
}
