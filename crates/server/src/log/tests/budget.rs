use super::*;

#[test]
fn admits_up_to_the_operation_count() {
    let mut budget = ReadBudget::ops(2);
    assert!(budget.admit(10));
    assert!(!budget.is_exhausted());
    assert!(budget.admit(10));
    assert!(budget.is_exhausted());
    assert!(!budget.admit(1));
}

#[test]
fn refuses_what_exceeds_the_bytes_and_stays_exhausted() {
    let mut budget = ReadBudget::new(10, 100);
    assert!(budget.admit(60));
    assert!(!budget.admit(50));
    assert!(budget.is_exhausted());
    // A smaller operation later would leave a hole where the refused one was.
    assert!(!budget.admit(1));
}

#[test]
fn always_admits_the_first_operation() {
    let mut budget = ReadBudget::new(10, 100);
    assert!(budget.admit(1_000));
    assert!(!budget.admit(1));
}

#[test]
fn zero_operations_admit_nothing() {
    let mut budget = ReadBudget::ops(0);
    assert!(budget.is_exhausted());
    assert!(!budget.admit(1));
}
