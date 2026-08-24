use common_game::{
    protocols::{orchestrator_explorer::*, planet_explorer::*},
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::Explorer as ExplorerTrait;
use explorer_common::{Bag, BagContent};
pub struct Explorer {
    id: ID,
    bag: Bag,
    planet_id: ID,
    auto_mode: bool,
    rx_planet: Receiver<PlanetToExplorer>,
    tx_planet: Sender<ExplorerToPlanet>,
    rx_orchestrator: Receiver<OrchestratorToExplorer>,
    tx_orchestrator: Sender<ExplorerToOrchestrator<BagContent>>,
}
impl Explorer {
    pub fn new(
        id: ID,
        bag: Bag,
        planet_id: ID,
        rx_planet: Receiver<PlanetToExplorer>,
        tx_planet: Sender<ExplorerToPlanet>,
        rx_orchestrator: Receiver<OrchestratorToExplorer>,
        tx_orchestrator: Sender<ExplorerToOrchestrator<BagContent>>,
    ) -> Self {
        Self {
            id,
            bag: bag,
            planet_id,
            auto_mode: false,
            rx_planet,
            tx_planet,
            rx_orchestrator,
            tx_orchestrator,
        }
    }
}
impl ExplorerTrait for Explorer {
    fn run(
        &mut self,
        rx_planet: Receiver<PlanetToExplorer>,
        rx_orchestrator: Receiver<OrchestratorToExplorer>,
        tx_orchestrator: Sender<ExplorerToOrchestrator<BagContent>>,
    ) {
        self.auto_mode = false;
        self.rx_planet = rx_planet;
        self.rx_orchestrator = rx_orchestrator;
        self.tx_orchestrator = tx_orchestrator;

        loop {
            self.try_recv_from_orchestrator_and_respond();
        }
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_bag(&mut self) -> &mut Bag {
        &mut self.bag
    }

    fn get_planet_id(&self) -> ID {
        self.planet_id
    }

    fn set_planet_id(&mut self, new: ID) {
        self.planet_id = new;
    }

    fn get_auto_mode(&self) -> bool {
        self.auto_mode
    }

    fn set_auto_mode(&mut self, mode: bool) {
        self.auto_mode = mode;
    }

    fn get_rx_planet(&self) -> Receiver<PlanetToExplorer> {
        self.rx_planet.clone()
    }

    fn set_rx_planet(&mut self, new: Receiver<PlanetToExplorer>) {
        self.rx_planet = new;
    }

    fn get_tx_planet(&self) -> Sender<ExplorerToPlanet> {
        self.tx_planet.clone()
    }

    fn set_tx_planet(&mut self, new: Sender<ExplorerToPlanet>) {
        self.tx_planet = new;
    }

    fn get_rx_orchestrator(&self) -> Receiver<OrchestratorToExplorer> {
        self.rx_orchestrator.clone()
    }

    fn set_rx_orchestrator(&mut self, new: Receiver<OrchestratorToExplorer>) {
        self.rx_orchestrator = new;
    }

    fn get_tx_orchestrator(&self) -> Sender<ExplorerToOrchestrator<BagContent>> {
        self.tx_orchestrator.clone()
    }

    fn set_tx_orchestrator(&mut self, new: Sender<ExplorerToOrchestrator<BagContent>>) {
        self.tx_orchestrator = new;
    }
}

// The tested functions were moved to explorer_common
#[cfg(test)]
mod tests {
    use std::{collections::HashSet, thread};

    use common_game::{
        components::resource::ComplexResourceType,
        protocols::planet_explorer::PlanetToExplorer::SupportedCombinationResponse,
    };

    use super::*;

    struct TestEnvironment {
        // Channel ends of the orchestrator to/from the explorer
        tx_orchestrator: Sender<OrchestratorToExplorer>,
        rx_orchestrator: Receiver<ExplorerToOrchestrator<BagContent>>,
        // Channel ends of the planet to/from the explorer
        tx_planet: Sender<PlanetToExplorer>,
        rx_planet: Receiver<ExplorerToPlanet>,

        explorer: Explorer,
    }

    impl Default for TestEnvironment {
        fn default() -> Self {
            let (tx_explorer_orchestrator, rx_explorer_orchestrator) =
                crossbeam_channel::unbounded();
            let (tx_orchestrator_explorer, rx_orchestrator_explorer) =
                crossbeam_channel::unbounded();
            let (tx_explorer_planet, rx_explorer_planet) = crossbeam_channel::unbounded();
            let (tx_planet_explorer, rx_planet_explorer) = crossbeam_channel::unbounded();

            let explorer = Explorer {
                id: 0,
                bag: Bag::new(),
                planet_id: 0,
                auto_mode: true,
                rx_planet: rx_planet_explorer,
                tx_planet: tx_explorer_planet,
                rx_orchestrator: rx_orchestrator_explorer,
                tx_orchestrator: tx_explorer_orchestrator,
            };

            Self {
                tx_orchestrator: tx_orchestrator_explorer,
                rx_orchestrator: rx_explorer_orchestrator,
                tx_planet: tx_planet_explorer,
                rx_planet: rx_explorer_planet,
                explorer,
            }
        }
    }
    #[test]
    fn test_is_combination_available() {
        let environment = TestEnvironment::default();
        let resource_type = ComplexResourceType::Diamond;
        let mut combination_list = HashSet::new();
        combination_list.insert(ComplexResourceType::Diamond);
        thread::scope(|t| {
            t.spawn(|| {
                if let Ok(msg) = environment.rx_planet.recv() {
                    if let ExplorerToPlanet::SupportedCombinationRequest { explorer_id } = msg {
                        if let Ok(()) = environment
                            .tx_planet
                            .send(SupportedCombinationResponse { combination_list })
                        {
                        }
                    }
                }
            });
            assert_eq!(
                environment.explorer.is_combination_available(resource_type),
                true
            );
        });
    }
}
