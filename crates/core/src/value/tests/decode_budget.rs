use super::*;

#[test]
fn values_are_unlimited_outside_a_budget() {
    for _ in 0..1000 {
        charge_values(1).unwrap();
    }
}

#[test]
fn a_budget_admits_exactly_its_values() {
    let budget = ValueBudget::start(3);
    for _ in 0..3 {
        charge_values(1).unwrap();
    }
    assert!(!budget.exhausted());
    assert_eq!(charge_values(1), Err(BudgetExhausted));
    assert!(budget.exhausted());
    // Once exhausted it stays exhausted until the guard is dropped.
    assert_eq!(charge_values(1), Err(BudgetExhausted));
    drop(budget);
    charge_values(1).unwrap();
}

#[test]
fn values_are_charged_in_bulk() {
    let budget = ValueBudget::start(10);
    charge_values(4).unwrap();
    charge_values(6).unwrap();
    assert_eq!(charge_values(1), Err(BudgetExhausted));
    assert!(budget.exhausted());

    // A charge over what is left fails whole.
    let budget = ValueBudget::start(10);
    assert_eq!(charge_values(11), Err(BudgetExhausted));
    assert!(budget.exhausted());
}

#[test]
fn dropping_the_guard_restores_the_previous_budget() {
    let outer = ValueBudget::start(1);
    {
        let inner = ValueBudget::start(0);
        assert!(charge_values(1).is_err());
        assert!(inner.exhausted());
    }
    assert!(!outer.exhausted());
    charge_values(1).unwrap();
    assert!(charge_values(1).is_err());
}

#[test]
fn a_panicking_decode_does_not_leave_its_budget_behind() {
    let result = std::panic::catch_unwind(|| {
        let _budget = ValueBudget::start(0);
        panic!("decode panicked");
    });
    assert!(result.is_err());
    charge_values(1).unwrap();
}
