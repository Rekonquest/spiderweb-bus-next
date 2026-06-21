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
//! - It is **not** drop-a-file-on-disk discovery. The binary must still
//!   list each strand crate as a Cargo **dependency** and keep it linked
//!   (a `use my_crate as _;` anchor) — the linker can dead-strip an advert
//!   from a crate it otherwise never references. `auto_connect` prints the
//!   collected count + names so a missing advert is obvious.
//! - The factory must be a bare `fn` pointer (no captured state), so
//!   per-instance config lives inside the factory body, not a closure.
//! - The kernel (`spiderweb`) gains **zero** dependencies and stays
//!   `forbid(unsafe_code)`; the `inventory` dependency (and its internal
//!   linker-section machinery) is quarantined here. Never move
//!   `advertise!` / `collect!` into the kernel.

#![forbid(unsafe_code)]

use spiderweb::{Bus, Strand, StrandSpec};

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
            }
        }
    };
}

/// Collect every advertised strand onto a [`Bus`].
pub trait BusAutoConnect {
    /// Register every advertised strand (and the advertised spider, if
    /// any) onto this bus, then return it for chaining. Reuses the
    /// kernel's existing `register_spec` / `register_spider` — so name
    /// collisions and the at-most-one-spider rule are enforced exactly
    /// as for manual registration, and auto-connect coexists with manual
    /// `register` (use distinct names). Prints the collected count and
    /// names so a missing advert (a strand whose crate the binary didn't
    /// link) is obvious.
    fn auto_connect(&mut self) -> &mut Self;
}

impl BusAutoConnect for Bus {
    fn auto_connect(&mut self) -> &mut Self {
        let mut names: Vec<&'static str> = Vec::new();
        let mut spiders = 0usize;
        for adv in advertised() {
            // The advert carries a Copy `fn` pointer; capture it into the
            // factory closure the kernel expects. The kernel is untouched.
            let factory = adv.factory;
            let spec = StrandSpec::new(adv.name, move || factory());
            match adv.role {
                Role::Strand => self.register_spec(spec),
                Role::Spider => {
                    self.register_spider(spec);
                    spiders += 1;
                }
            }
            names.push(adv.name);
        }
        println!(
            "[auto-connect] {} advertised: [{}]{}",
            names.len(),
            names.join(", "),
            if spiders > 0 { " + spider" } else { "" }
        );
        self
    }
}
