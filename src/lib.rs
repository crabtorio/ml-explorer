use common_game::{
    components::resource::{
        ComplexResourceRequest, ComplexResourceType,
        GenericResource::{self, BasicResources},
    },
    protocols::{
        orchestrator_explorer::{ExplorerToOrchestrator::*, OrchestratorToExplorer::*, *},
        planet_explorer::{PlanetToExplorer::*, *},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::Bag;
use explorer_common::Explorer as ExplorerTrait;
struct Explorer {
    id: ID,
    bag: Bag,
    planet_id: ID,
    auto_mode: bool,
    rx_planet: Receiver<PlanetToExplorer>,
    tx_planet: Sender<ExplorerToPlanet>,
    rx_orchestrator: Receiver<OrchestratorToExplorer>,
    tx_orchestrator: Sender<ExplorerToOrchestrator<Bag>>,
}
impl Explorer {
    pub fn is_combination_available(&self, resource: ComplexResourceType) -> bool {
        if let Ok(()) = self
            .tx_planet
            .send(ExplorerToPlanet::SupportedCombinationRequest {
                explorer_id: self.id,
            })
        {
            if let Ok(msg) = self.rx_planet.recv() {
                if let SupportedCombinationResponse { combination_list } = msg {
                    return combination_list.contains(&resource);
                }
            }
        }
        false
    }
}
impl ExplorerTrait for Explorer {
    fn run(&mut self) {
        self.auto_mode = false;
        loop {
            // Checks for a message from the orchestrator
            if let Ok(message) = self.rx_orchestrator.try_recv() {
                match message {
                    StartExplorerAI => {
                        self.auto_mode = true;
                    }
                    ResetExplorerAI => self.auto_mode = true,
                    KillExplorer => break,
                    StopExplorerAI => self.auto_mode = false,
                    MoveToPlanet {
                        sender_to_new_planet,
                        planet_id,
                    } => {
                        self.planet_id = planet_id;
                        if let Some(new_sender) = sender_to_new_planet {
                            self.tx_planet = new_sender;
                            match self.tx_orchestrator.send(MovedToPlanetResult {
                                explorer_id: self.id,
                                planet_id,
                            }) {
                                _ => (), // Logging
                            }
                        }
                    }
                    CurrentPlanetRequest => {
                        if let Ok(()) = self.tx_orchestrator.send(CurrentPlanetResult {
                            explorer_id: self.id,
                            planet_id: self.planet_id,
                        }) {
                            // Logging
                        }
                    }
                    SupportedResourceRequest => {
                        if let Ok(()) =
                            self.tx_planet
                                .send(ExplorerToPlanet::SupportedResourceRequest {
                                    explorer_id: self.id,
                                })
                        {
                            if let Ok(msg) = self.rx_planet.recv() {
                                if let SupportedResourceResponse { resource_list } = msg {
                                    if let Ok(()) =
                                        self.tx_orchestrator.send(SupportedResourceResult {
                                            explorer_id: self.id,
                                            supported_resources: resource_list,
                                        })
                                    {
                                        // Logging
                                    }
                                }
                            }
                        }
                    }
                    SupportedCombinationRequest => {
                        if let Ok(()) =
                            self.tx_planet
                                .send(ExplorerToPlanet::SupportedCombinationRequest {
                                    explorer_id: self.id,
                                })
                        {
                            if let Ok(msg) = self.rx_planet.recv() {
                                if let SupportedCombinationResponse { combination_list } = msg {
                                    if let Ok(()) =
                                        self.tx_orchestrator.send(SupportedCombinationResult {
                                            explorer_id: self.id,
                                            combination_list,
                                        })
                                    {
                                        // Logging
                                    }
                                }
                            }
                        }
                    }
                    OrchestratorToExplorer::GenerateResourceRequest { to_generate } => {
                        if let Ok(()) =
                            self.tx_planet
                                .send(ExplorerToPlanet::GenerateResourceRequest {
                                    explorer_id: self.id,
                                    resource: to_generate,
                                })
                        {
                            if let Ok(msg) = self.rx_planet.recv() {
                                if let PlanetToExplorer::GenerateResourceResponse { resource } = msg
                                {
                                    if let Some(resource) = resource {
                                        if let Ok(()) = self.tx_orchestrator.send(
                                            ExplorerToOrchestrator::GenerateResourceResponse {
                                                explorer_id: self.id,
                                                generated: Ok(()),
                                            },
                                        ) {
                                            self.bag.resources.push(BasicResources(resource));
                                        }
                                    } else {
                                        if let Ok(()) = self.tx_orchestrator.send(
                                            ExplorerToOrchestrator::GenerateResourceResponse {
                                                explorer_id: self.id,
                                                generated: Err(String::from(
                                                    "No resource was created",
                                                )),
                                            },
                                        ) {}
                                    }
                                }
                            }
                        }
                    }
                    OrchestratorToExplorer::CombineResourceRequest { to_generate } => {}
                    BagContentRequest => todo!(),
                    NeighborsResponse { neighbors } => todo!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, thread};

    use super::*;

    struct TestEnvironment {
        // Channel ends of the orchestrator to/from the explorer
        tx_orchestrator: Sender<OrchestratorToExplorer>,
        rx_orchestrator: Receiver<ExplorerToOrchestrator<Bag>>,
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
