use common_game::{
    components::{
        planet,
        resource::{
            self,
            GenericResource::{self, BasicResources},
        },
    },
    protocols::{
        orchestrator_explorer::{ExplorerToOrchestrator::*, OrchestratorToExplorer::*, *},
        planet_explorer::{ExplorerToPlanet::*, PlanetToExplorer::*, *},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
struct Bag {
    resources: Vec<GenericResource>,
}

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
                    OrchestratorToExplorer::CombineResourceRequest { to_generate } => todo!(),
                    BagContentRequest => todo!(),
                    NeighborsResponse { neighbors } => todo!(),
                }
            }
        }
    }
}
