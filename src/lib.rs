use std::collections::HashSet;

use common_game::{
    components::resource::{
        BasicResource,
        BasicResourceType::{self, Carbon},
        ComplexResourceRequest,
        ComplexResourceType::{self, Diamond},
        GenericResource, ResourceType,
    },
    protocols::{
        orchestrator_explorer::*,
        planet_explorer::{ExplorerToPlanet, PlanetToExplorer},
    },
    utils::ID,
};
use crossbeam_channel::{Receiver, Sender};
use explorer_common::{AiReturn, Bag, BagContent};
use explorer_common::{Explorer as ExplorerTrait, logged_channel::LoggedChannel};
const GOAL: ComplexResourceType = Diamond;
const RECIPE: (BasicResourceType, BasicResourceType) = (Carbon, Carbon);
pub struct Explorer {
    id: ID,
    bag: Bag,
    planet_id: ID,
    auto_mode: bool,
    planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
    orchestrator_channel: LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer>,
    visited: HashSet<ID>,
    planet_stack: Vec<ID>,
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
    fn supported_resource_request(&self) -> Result<HashSet<BasicResourceType>, AiReturn> {
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
        Err(AiReturn::Kill)
    }
    fn supported_combination_request(&self) -> Result<HashSet<ComplexResourceType>, AiReturn> {
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
        Err(AiReturn::Kill)
    }
    fn generate_resource_request(
        &self,
        resource: BasicResourceType,
    ) -> Result<Option<BasicResource>, AiReturn> {
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
                return Ok(resource);
            }
        }
        Err(AiReturn::Kill)
    }
    fn travel_to_planet_request(&mut self, dst_planet_id: ID) -> Result<bool, AiReturn> {
        if let Ok(()) =
            self.orchestrator_channel
                .send(ExplorerToOrchestrator::TravelToPlanetRequest {
                    explorer_id: self.id,
                    current_planet_id: self.planet_id,
                    dst_planet_id,
                })
        {
            if let Ok(message) = self.orchestrator_channel.recv() {
                match message {
                    OrchestratorToExplorer::MoveToPlanet {
                        sender_to_new_planet,
                        planet_id,
                    } => {
                        if let Some(new_sender) = sender_to_new_planet {
                            self.set_planet_channel_tx(new_sender);
                            self.planet_id = planet_id;

                            if let Ok(()) = self.orchestrator_channel.send(
                                ExplorerToOrchestrator::MovedToPlanetResult {
                                    explorer_id: self.id,
                                    planet_id,
                                },
                            ) {
                            } else {
                                return Err(AiReturn::Kill);
                            }

                            return Ok(true);
                        } else {
                            return Ok(false); // the planet is dead
                        }
                    }
                    OrchestratorToExplorer::StopExplorerAI => return Err(AiReturn::Stop),
                    OrchestratorToExplorer::ResetExplorerAI => return Err(AiReturn::Reset),
                    OrchestratorToExplorer::KillExplorer => return Err(AiReturn::Kill),
                    _ => return Err(AiReturn::Kill),
                }
            } else {
                return Err(AiReturn::Kill);
            }
        } else {
            return Err(AiReturn::Kill);
        }
    }
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
        loop {
            // If planet is new set it as visited (doesn't need check as worst case it re-enters it, which is safe)
            self.visited.insert(self.planet_id);

            // Get planet's supported resources and combinations
            let supported_resources = {
                match self.supported_resource_request() {
                    Ok(supported_resources) => supported_resources,
                    Err(ai_return) => return ai_return,
                }
            };
            let supported_combinations = {
                match self.supported_combination_request() {
                    Ok(supported_combinations) => supported_combinations,
                    Err(ai_return) => return ai_return,
                }
            };

            // Check if there is anything needed given the goal (Diamond)
            if self.bag.contains(ResourceType::Basic(Carbon)) >= 2 {
                if supported_combinations.contains(&Diamond) {
                    let carbon1 = self.bag.take_resource(ResourceType::Basic(Carbon));
                    let carbon2 = self.bag.take_resource(ResourceType::Basic(Carbon));

                    match (carbon1, carbon2) {
                        (
                            Ok(GenericResource::BasicResources(BasicResource::Carbon(carbon1))),
                            Ok(GenericResource::BasicResources(BasicResource::Carbon(carbon2))),
                        ) => {
                            if let Ok(()) =
                                self.planet_channel
                                    .send(ExplorerToPlanet::CombineResourceRequest {
                                        explorer_id: self.id,
                                        msg: ComplexResourceRequest::Diamond(carbon1, carbon2),
                                    })
                            {
                                // Missing response
                                if let Ok(PlanetToExplorer::CombineResourceResponse {
                                    complex_response,
                                }) = self.planet_channel.recv()
                                {
                                    match complex_response {
                                        Ok(diamond) => {
                                            println!("I GOT THE DIAMOND!!!!");
                                            return AiReturn::Kill;
                                        }
                                        Err((_, carbon1, carbon2)) => {
                                            // Error ignored
                                            self.bag.add_resource(carbon1);
                                            self.bag.add_resource(carbon2);
                                        }
                                    }
                                } else {
                                    // Carbon theft
                                }
                            }
                        }
                        (
                            Ok(GenericResource::BasicResources(BasicResource::Carbon(carbon1))),
                            Err(_),
                        ) => {
                            self.bag.add_resource(GenericResource::BasicResources(
                                BasicResource::Carbon(carbon1),
                            ));
                        }
                        (
                            Err(_),
                            Ok(GenericResource::BasicResources(BasicResource::Carbon(carbon2))),
                        ) => {
                            self.bag.add_resource(GenericResource::BasicResources(
                                BasicResource::Carbon(carbon2),
                            ));
                        }
                        _ => {}
                    }
                }
            } else {
                // Try to get Carbon
                if supported_resources.contains(&Carbon) {
                    match self.generate_resource_request(Carbon) {
                        Ok(Some(carbon)) => self
                            .bag
                            .add_resource(GenericResource::BasicResources(carbon)),
                        Ok(None) => (), // Missed the charged energy cell, do nothing
                        Err(ai_return) => return ai_return,
                    }
                }
            }

            // If any neighbor hasn't been visited, move there
            match self.neighbors_request() {
                Ok(neighbors) => {
                    if !neighbors.is_empty() {
                        if let Some(unvisited_planet) = neighbors.iter().find(|neighbor| {
                            self.visited
                                .iter()
                                .all(|visited_planet| *neighbor != visited_planet)
                        }) {
                            let old_planet_id = self.planet_id;
                            match self.travel_to_planet_request(*unvisited_planet) {
                                Ok(did_it_move) => {
                                    if did_it_move {
                                        self.planet_stack.push(old_planet_id);
                                    } else {
                                        continue; // If the planet is dead, then the next iteration will not have it in the neighbors
                                    }
                                }
                                Err(ai_return) => return ai_return,
                            }
                        } else {
                            // If no neighbors are new and the stack is empty, then we have explored through the whole galaxy
                            // And since the objective hasn't been fullfilled yet, empty the visited map and explore it again
                            if self.planet_stack.is_empty() {
                                self.visited.clear();
                            } else {
                                // If no neighbors are new and there is stuff on the stack, backtrack
                                let dst = self.planet_stack.pop().unwrap(); // Cannot panic it was checked to be non-empty
                                match self.travel_to_planet_request(dst) {
                                    Ok(did_we_move) => {
                                        if !did_we_move {
                                            // The planet is dead, so we're cut out from the way we came from
                                            // Wipe visited and start exploring again
                                            self.visited.clear();
                                            self.planet_stack.clear();
                                        }
                                    }
                                    Err(ai_return) => return ai_return,
                                }
                            }
                        }
                    } else {
                        // If there are no neighbors then the explorer might as well die
                        return AiReturn::Kill;
                    }
                }
                Err(ai_return) => return ai_return,
            }
        }
    }
    fn reset(&mut self) {
        self.planet_stack.clear();
        self.visited.clear();
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
