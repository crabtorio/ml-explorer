use std::collections::HashSet;

use common_game::{
    components::resource::{BasicResource, BasicResourceType, ComplexResourceType},
    protocols::{
        orchestrator_explorer::*,
        planet_explorer::{ExplorerToPlanet, PlanetToExplorer},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::{AiReturn, Bag, BagContent};
use explorer_common::{Explorer as ExplorerTrait, logged_channel::LoggedChannel};
pub struct Explorer {
    id: ID,
    bag: Bag,
    planet_id: ID,
    auto_mode: bool,
    planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
    orchestrator_channel: LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer>,
    visited: HashSet<ID>,
    planet_stack: Vec<PlanetInfo>,
}
struct PlanetInfo {
    id: ID,
    supported_resources: HashSet<BasicResourceType>,
    supported_combinations: HashSet<ComplexResourceType>,
}
impl PlanetInfo {
    fn new(
        id: ID,
        supported_resources: HashSet<BasicResourceType>,
        supported_combinations: HashSet<ComplexResourceType>,
    ) -> Self {
        Self {
            id,
            supported_resources,
            supported_combinations,
        }
    }
}
impl Explorer {
    fn neighbors_request(&self) -> Result<Vec<ID>, AiReturn> {
        if let Ok(()) = self
            .orchestrator_channel
            .send(ExplorerToOrchestrator::NeighborsRequest {
                explorer_id: self.id,
                current_planet_id: self.planet_id,
            })
        {
            if let Ok(message) = self.orchestrator_channel.recv() {
                match message {
                    OrchestratorToExplorer::NeighborsResponse { neighbors } => {
                        return Ok(neighbors);
                    }
                    OrchestratorToExplorer::StopExplorerAI => return Err(AiReturn::Stop),
                    OrchestratorToExplorer::ResetExplorerAI => return Err(AiReturn::Reset),
                    OrchestratorToExplorer::KillExplorer => return Err(AiReturn::Kill),
                    _ => (),
                }
            }
        }
        Err(AiReturn::Kill)
    }
    fn travel_to_planet_request(
        &self,
        dst_planet_id: ID,
    ) -> Result<Option<Sender<ExplorerToPlanet>>, AiReturn> {
        if let Ok(()) =
            self.orchestrator_channel
                .send(ExplorerToOrchestrator::TravelToPlanetRequest {
                    explorer_id: self.id,
                    current_planet_id: self.planet_id,
                    dst_planet_id: dst_planet_id,
                })
        {
            if let Ok(message) = self.orchestrator_channel.recv() {
                match message {
                    OrchestratorToExplorer::MoveToPlanet {
                        sender_to_new_planet,
                        planet_id,
                    } => {
                        return Ok(sender_to_new_planet);
                    }
                    OrchestratorToExplorer::StopExplorerAI => return Err(AiReturn::Stop),
                    OrchestratorToExplorer::ResetExplorerAI => return Err(AiReturn::Reset),
                    OrchestratorToExplorer::KillExplorer => return Err(AiReturn::Kill),
                    _ => (),
                }
            }
        }
        Err(AiReturn::Kill)
    }
    fn supported_resource_request(&self) -> Result<HashSet<BasicResourceType>, ()> {
        if let Ok(()) = self
            .planet_channel
            .send(ExplorerToPlanet::SupportedResourceRequest {
                explorer_id: self.id,
            })
        {
            if let Ok(PlanetToExplorer::SupportedResourceResponse { resource_list }) =
                self.planet_channel.recv()
            {
                return Ok(resource_list);
            }
        }
        Err(())
    }
    fn supported_combination_request(&self) -> Result<HashSet<ComplexResourceType>, ()> {
        if let Ok(()) = self
            .planet_channel
            .send(ExplorerToPlanet::SupportedCombinationRequest {
                explorer_id: self.id,
            })
        {
            if let Ok(PlanetToExplorer::SupportedCombinationResponse { combination_list }) =
                self.planet_channel.recv()
            {
                return Ok(combination_list);
            }
        }
        Err(())
    }
    fn generate_resource_request(&self, resource: BasicResourceType) -> Option<BasicResource> {
        if let Ok(()) = self
            .planet_channel
            .send(ExplorerToPlanet::GenerateResourceRequest {
                explorer_id: self.id,
                resource: resource,
            })
        {
            if let Ok(PlanetToExplorer::GenerateResourceResponse { resource }) =
                self.planet_channel.recv()
            {
                return resource;
            }
        }
        None
    }
    /*fn combine_resource_request(
        &self,
        resource_request: ComplexResourceRequest,
    ) -> Result<ComplexResource, (String, GenericResource, GenericResource)> {
        if let Ok(()) = self
            .planet_channel
            .send(ExplorerToPlanet::CombineResourceRequest {
                explorer_id: self.id,
                msg: match  ,
            })
        {
            if let Ok(PlanetToExplorer::CombineResourceResponse { complex_response }) =
                self.planet_channel.recv()
            {
                return complex_response;
            }
        }
        panic!("");
    }*/
}
impl ExplorerTrait for Explorer {
    fn new(
        id: ID,
        bag: Bag,
        planet_id: ID,
        planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
        orchestrator_channel: LoggedChannel<
            ExplorerToOrchestrator<BagContent>,
            OrchestratorToExplorer,
        >,
    ) -> Self {
        Self {
            id,
            bag,
            planet_id,
            auto_mode: false,
            planet_channel,
            orchestrator_channel,
            planet_stack: Vec::new(),
            visited: HashSet::new(),
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

    fn get_planet_channel(&self) -> LoggedChannel<ExplorerToPlanet, PlanetToExplorer> {
        self.planet_channel.clone()
    }
    fn set_planet_channel_tx(&mut self, tx: Sender<ExplorerToPlanet>) {
        self.planet_channel.set_sender(tx);
    }
    fn set_planet_channel_rx(&mut self, rx: Receiver<PlanetToExplorer>) {
        self.planet_channel.set_receiver(rx);
    }

    fn get_orchestrator_channel(
        &self,
    ) -> LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer> {
        self.orchestrator_channel.clone()
    }
    fn set_orchestrator_channel_tx(&mut self, tx: Sender<ExplorerToOrchestrator<BagContent>>) {
        self.orchestrator_channel.set_sender(tx);
    }
    fn set_orchestrator_channel_rx(&mut self, rx: Receiver<OrchestratorToExplorer>) {
        self.orchestrator_channel.set_receiver(rx);
    }

    fn explorer_ai(&mut self) -> explorer_common::AiReturn {
        // if this planet is new
        if !self.visited.contains(&self.planet_id) {
            // Gets supported resources and combinations from the planet it is in
            if let (Ok(supported_resources), Ok(supported_combination)) = (
                self.supported_resource_request(),
                self.supported_combination_request(),
            ) {
                // Adds planet to the stack
                self.planet_stack.push(PlanetInfo::new(
                    self.planet_id,
                    supported_resources,
                    supported_combination,
                ));
                // Marks planet as visited
                self.visited.insert(self.planet_id);
            }
        }
        // Gets current planet's neighbours from orchestrator
        // and move to an unvisited planet, if there is one
        if let Ok(neighbors) = self.neighbors_request() {}

        AiReturn::Kill
    }

    fn reset(&mut self) {
        todo!()
    }
}

// The tested functions were moved to explorer_common
#[cfg(test)]
mod tests {
    use std::{collections::HashSet, thread};

    use common_game::{
        components::resource::ComplexResourceType, logging::Participant,
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
                planet_channel: LoggedChannel::new(
                    rx_planet_explorer,
                    tx_explorer_planet,
                    Participant::new(common_game::logging::ActorType::Explorer, 0 as ID),
                    Participant::new(common_game::logging::ActorType::Planet, 0 as ID),
                    common_game::logging::EventType::MessageExplorerToPlanet,
                    common_game::logging::EventType::MessagePlanetToExplorer,
                ),
                orchestrator_channel: LoggedChannel::new(
                    rx_orchestrator_explorer,
                    tx_explorer_orchestrator,
                    Participant::new(common_game::logging::ActorType::Explorer, 0 as ID),
                    Participant::new(common_game::logging::ActorType::Orchestrator, 0 as ID),
                    common_game::logging::EventType::MessageExplorerToOrchestrator,
                    common_game::logging::EventType::MessageOrchestratorToExplorer,
                ),
                planet_stack: Vec::new(),
                visited: HashSet::new(),
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
