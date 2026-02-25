use super::render::format_value;

#[test]
fn format_value_integers() {
    assert_eq!(format_value(0.0), "0");
    assert_eq!(format_value(42.0), "42");
    assert_eq!(format_value(1000.0), "1000");
}

#[test]
fn format_value_floats() {
    #[allow(clippy::approx_constant)]
    let pi_ish = 3.14;
    assert_eq!(format_value(pi_ish), "3.14");
    assert_eq!(format_value(0.5), "0.5");
}
