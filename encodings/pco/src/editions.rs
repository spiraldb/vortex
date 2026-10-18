// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The `pco` edition family.
//!
//! The v1 `vortex.pco` format is a member of `core`. This family holds the draft v2 format, which
//! must be enabled explicitly until it is promoted.

use vortex_edition::Edition;
use vortex_edition::EditionDeclaration;
use vortex_edition::EditionFamily;
use vortex_edition::EditionId;
use vortex_edition::EditionMember;

/// The `pco` family: Pco wire formats that are not yet part of `core`.
pub static FAMILY: EditionFamily = EditionFamily {
    name: "pco",
    origin: "vortex-pco",
    doc: "Pco wire formats under testing before promotion to core. `vortex.pco.v2` adds a \
random-access index of mid-page decoder checkpoints and block statistics to the frozen \
`vortex.pco` format; dropping the index leaves valid `vortex.pco` data.",
};

/// The October 2026 draft edition of the `pco` family.
pub const PCO_2026_10: EditionId = EditionId::new("pco", 2026, 10, 0);

/// The declaration of [`PCO_2026_10`] and the components that join the family at it.
pub static DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: PCO_2026_10,
        min_library_version: None,
    },
    added: &[EditionMember::array(&"vortex.pco.v2")],
};

#[cfg(test)]
mod tests {
    use vortex_edition::EditionSessionExt;
    use vortex_edition::test_harness::validate_edition;
    use vortex_error::VortexResult;

    use super::*;

    #[test]
    fn pco_edition_is_valid() -> VortexResult<()> {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        validate_edition(&session.editions(), &PCO_2026_10)
    }

    #[test]
    fn initialize_does_not_enable_pco_edition() {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        assert!(!session.enabled_editions().editions().contains(&PCO_2026_10));
    }
}
