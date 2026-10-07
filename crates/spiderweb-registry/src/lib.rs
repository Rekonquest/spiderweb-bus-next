//! `spiderweb-registry` — self-registration for the spiderweb bus.
//!
//! Strands advertise themselves; the bus auto-connects them. No central
//! `main()` list. This is the "crates advertise themselves to the bus"
//! half of the doctrine: a crate deposits a capability advert into a
//! static slice ([`advertise!`]), the bus collects them all at boot
//! ([`BusAutoConnect::auto_connect`]), and the spider orchestrates the
//! advertised set over the unchanged `sys.lifecycle` stream.
//!
//! ## What a strand author writes
//!
//! One line, at module scope, no `main()` edit:
//!
//! ```ignore
//! spiderweb_registry::advertise!("my-strand", || Box::new(MyStrand::new()) as Box<dyn spiderweb::Strand>);
//! ```
//!
//! ## What the binary writes
//!
//! ```ignore
//! let mut bus = spiderweb::Bus::open();
//! bus.auto_connect();          // registers everything advertised
//! bus.run_until(Some(dur));
//! ```
//!
//! ## Honest caveats
//!
//! - **Connection by type is already automatic** (the kernel routes by
//!   `TypeId`); this removes the last manual step — *announcing* a strand.
//! - It is **not** drop-a-file-on-disk discovery. The binary must depend
//!   on each strand crate AND actually *reference* it. The Cargo
//!   dependency is **necessary but not sufficient** — a crate that is only
//!   a dependency, with no symbol referenced, gets dead-stripped by the
//!   linker, advert and all: it compiles, runs, exits 0, and the strand is
//!   silently absent. The **load-bearing part is the anchor**:
//!   `use my_crate as _;` in the binary. Use
//!   [`BusAutoConnect::auto_connect_expecting`] to turn a missing advert
//!   into a loud panic instead of a silent no-op.
//! - The factory must be a bare `fn` pointer (no captured state), so
//!   per-instance config lives inside the factory body, not a closure.
//! - The kernel (`spiderweb`) gains **zero** dependencies and stays
//!   `forbid(unsafe_code)`; the `inventory` dependency (and its internal
//!   linker-section machinery) is quarantined here. Never move
//!   `advertise!` / `collect!` into the kernel.

#![forbid(unsafe_code)]

use spiderweb::{Bus, Position, Strand, StrandSpec};

/// Re-export so the [`advertise!`] macro can reach `inventory::submit!`
/// from any crate that depends only on `spiderweb-registry`.
pub use inventory;

/// Whether an advertised strand is an ordinary strand or the single
/// orchestrator (spider).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Strand,
    Spider,
}

/// A capability a crate advertises to the bus: a unique name, a
/// parameterless factory that builds the strand, and its role. Built
/// at item scope by [`advertise!`] / [`advertise_spider!`] and collected
/// at boot by [`BusAutoConnect::auto_connect`].
pub struct StrandAdvert {
    pub name: &'static str,
    pub factory: fn() -> Box<dyn Strand>,
    pub role: Role,
    /// Optional 3D position the node advertises (v1.3). `None` = unplaced.
    /// `auto_connect` forwards a `Some` to `Bus::place`.
    pub position: Option<Position>,
}

inventory::collect!(StrandAdvert);

/// Every advert the linked crates submitted, in link order (do not
/// depend on the order — names are unique and collision-checked).
pub fn advertised() -> impl Iterator<Item = &'static StrandAdvert> {
    inventory::iter::<StrandAdvert>.into_iter()
}

/// Advertise a strand to the bus. Write this once, at module scope, in
/// the strand's crate — no `main()` edit. The factory is a bare `fn`
/// pointer (typically a closure capturing nothing):
///
/// ```ignore
/// spiderweb_registry::advertise!("cap-source", || Box::new(CapSource::new()) as Box<dyn spiderweb::Strand>);
/// ```
#[macro_export]
macro_rules! advertise {
    ($name:expr, $factory:expr $(,)?) => {
        $crate::inventory::submit! {
            $crate::StrandAdvert {
                name: $name,
                factory: $factory,
                role: $crate::Role::Strand,
                position: ::core::option::Option::None,
            }
        }
    };
}

/// Advertise a strand WITH a 3D position (v1.3). `auto_connect` forwards
/// the position to `Bus::place`, so the node appears in the geometric
/// fabric snapshot. `pos` is a `spiderweb::Position`.
#[macro_export]
macro_rules! advertise_at {
    ($name:expr, $factory:expr, $pos:expr $(,)?) => {
        $crate::inventory::submit! {
            $crate::StrandAdvert {
                name: $name,
                factory: $factory,
                role: $crate::Role::Strand,
                position: ::core::option::Option::Some($pos),
            }
        }
    };
}

/// Advertise the orchestrator (spider). At most one may be advertised
/// across the whole binary; a second makes `auto_connect` panic, exactly
/// as `Bus::register_spider` does today.
#[macro_export]
macro_rules! advertise_spider {
    ($name:expr, $factory:expr $(,)?) => {
        $crate::inventory::submit! {
            $crate::StrandAdvert {
                name: $name,
                factory: $factory,
                role: $crate::Role::Spider,
                position: ::core::option::Option::None,
            }
        }
    };
}

/// Render the boot diagnostic. Pure so it can be unit-tested; the
/// strand count and the spider are reported separately (never double
/// counted).
fn summary(strand_names: &[&str], spider: Option<&str>) -> String {
    if strand_names.is_empty() && spider.is_none() {
        return "[auto-connect] WARNING: nothing advertised — did you forget the \
                `use <crate> as _;` link anchors (or the Cargo dependencies)?"
            .to_string();
    }
    let mut s = format!(
        "[auto-connect] {} strand(s): [{}]",
        strand_names.len(),
        strand_names.join(", ")
    );
    if let Some(sp) = spider {
        s.push_str(&format!(" + spider \"{sp}\""));
    }
    s
}

/// Collect advertised strands onto a [`Bus`].
///
/// Reuses the kernel's existing `register_spec` / `register_spider`, so
/// name collisions and the at-most-one-spider rule are enforced exactly
/// as for manual registration. **Pick one registration path per strand:**
/// a crate that self-advertises should not ALSO be manually `register`ed
/// on a bus you `auto_connect` — that is a duplicate name and panics, and
/// you usually can't make the names distinct (e.g. `cap-source`'s `name()`
/// is fixed). The single-spider limit likewise spans both paths: advertise
/// the spider or manually register it, never both.
pub trait BusAutoConnect {
    /// Register every advertised strand (and the advertised spider, if
    /// any) onto this bus, then return it for chaining. The diagnostic
    /// (count + names) goes to **stderr**; a missing advert (a strand
    /// whose crate the binary didn't *link* — see the crate docs on the
    /// `use crate as _;` anchor) shows up as a smaller-than-expected
    /// count, so prefer [`auto_connect_expecting`](Self::auto_connect_expecting)
    /// when you know what must be present.
    fn auto_connect(&mut self) -> &mut Self;

    /// Like [`auto_connect`](Self::auto_connect) but **panics loudly** if
    /// any `required` strand was not advertised — turning a silent
    /// dead-stripped advert into a clear failure with an anchor hint.
    fn auto_connect_expecting(&mut self, required: &[&str]) -> &mut Self;

    /// Register an explicit set of adverts (the testable seam
    /// [`auto_connect`](Self::auto_connect) delegates to). Lets callers
    /// drive a known advert set without the process-global slice.
    fn auto_connect_from<'a, I>(&mut self, adverts: I) -> &mut Self
    where
        I: IntoIterator<Item = &'a StrandAdvert>;
}

impl BusAutoConnect for Bus {
    fn auto_connect(&mut self) -> &mut Self {
        self.auto_connect_from(advertised())
    }

    fn auto_connect_expecting(&mut self, required: &[&str]) -> &mut Self {
        // Check presence BEFORE registering so a missing strand fails
        // before any side effects.
        let present: std::collections::BTreeSet<&str> = advertised().map(|a| a.name).collect();
        let missing: Vec<&str> = required
            .iter()
            .copied()
            .filter(|n| !present.contains(n))
            .collect();
        assert!(
            missing.is_empty(),
            "auto_connect_expecting: these strands were not advertised: {missing:?} — \
             did you forget a `use <crate> as _;` link anchor (the Cargo dependency \
             alone is not enough; the linker can dead-strip an unreferenced advert)?"
        );
        self.auto_connect()
    }

    fn auto_connect_from<'a, I>(&mut self, adverts: I) -> &mut Self
    where
        I: IntoIterator<Item = &'a StrandAdvert>,
    {
        let mut strand_names: Vec<&str> = Vec::new();
        let mut spider_name: Option<&str> = None;
        for adv in adverts {
            // The advert carries a Copy `fn` pointer; capture it into the
            // factory closure the kernel expects. The kernel is untouched.
            let factory = adv.factory;
            let spec = StrandSpec::new(adv.name, move || factory());
            match adv.role {
                Role::Strand => {
                    self.register_spec(spec);
                    strand_names.push(adv.name);
                }
                Role::Spider => {
                    self.register_spider(spec);
                    spider_name = Some(adv.name);
                }
            }
            // Forward an advertised position into the geometric fabric.
            // (Bus::place may still override it afterward — operator wins.)
            if let Some(pos) = adv.position {
                self.place(adv.name, pos);
            }
        }
        eprintln!("{}", summary(&strand_names, spider_name));
        self
    }
}
