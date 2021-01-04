use std::sync::Arc;
use std::time::Duration;

use waitmap::WaitMap;

use async_std::task;

#[test]
fn works_like_a_normal_map() {
    let map = WaitMap::new();
    assert!(map.get("Rosa Luxemburg").is_none());
    map.insert(String::from("Rosa Luxemburg"), 0);
    assert_eq!(map.get("Rosa Luxemburg").unwrap().value(), &0);
    assert!(map.get("Voltairine de Cleyre").is_none());
}

#[test]
fn simple_waiting() {
    let map: Arc<WaitMap<String, i32>> = Arc::new(WaitMap::new());
    let map2 = map.clone();

    let handle = task::spawn(async move {
        let rosa = map.wait("Rosa Luxemburg").await;
        assert_eq!(rosa.unwrap().value(), &0);
        assert!(map.wait("Voltairine de Cleyre").await.is_none());
    });

    task::spawn(async move {
        task::sleep(Duration::from_millis(140)).await;
        map2.insert(String::from("Rosa Luxemburg"), 0);
        task::sleep(Duration::from_millis(140)).await;
        map2.cancel("Voltairine de Cleyre");
    });

    task::block_on(handle);
}

#[test]
fn simple_waiting_mut() {
    let map: Arc<WaitMap<String, i32>> = Arc::new(WaitMap::new());
    let map2 = map.clone();

    let handle = task::spawn(async move {
        let rosa = map.wait_mut("Rosa Luxemburg").await;
        assert_eq!(rosa.unwrap().value(), &0);
        assert!(map.wait_mut("Voltairine de Cleyre").await.is_none());
    });

    task::spawn(async move {
        task::sleep(Duration::from_millis(140)).await;
        map2.insert(String::from("Rosa Luxemburg"), 0);
        task::sleep(Duration::from_millis(140)).await;
        map2.cancel("Voltairine de Cleyre");
    });

    task::block_on(handle);
}

#[test]
fn cancel_all_cancels_all() {
    let map: Arc<WaitMap<String, i32>> = Arc::new(WaitMap::new());
    let map2 = map.clone();

    let handle = task::spawn(async move {
        let rosa = map.wait("Rosa Luxemburg");
        let voltairine = map.wait("Voltairine de Cleyre");
        assert!(rosa.await.is_none());
        assert!(voltairine.await.is_none());
    });

    task::spawn(async move {
        task::sleep(Duration::from_millis(140)).await;
        map2.cancel_all();
    });

    task::block_on(handle);
}

#[test]
fn multiple_tasks_can_wait_one_key() {
    let map: Arc<WaitMap<String, i32>> = Arc::new(WaitMap::new());
    let map1 = map.clone();
    let map2 = map.clone();

    task::spawn(async move {
        map.insert(String::from("Rosa Luxemburg"), 0);
    });

    let handle1 = task::spawn(async move {
        let rosa = map1.wait("Rosa Luxemburg").await;
        assert_eq!(rosa.unwrap().value(), &0);
    });

    let handle2 = task::spawn(async move {
        let rosa = map2.wait("Rosa Luxemburg").await;
        assert_eq!(rosa.unwrap().value(), &0);
    });

    task::block_on(handle1);
    task::block_on(handle2);
}
